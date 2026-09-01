"""Vault dataset downloads through the Creem API.

Vault metadata and vault share price history are commercial datasets. They are
**not** served by the main Trading Strategy oracle API, and they do not use the
main API key:

- The main API, :py:class:`tradingstrategy.client.Client`, authenticates with a
  ``secret-token:`` prefixed key sent in the ``Authorization`` header.

- Vault datasets are sold through `Creem <https://creem.io>`__ and served by a
  Cloudflare Worker sitting in front of the dataset storage. The Worker
  authenticates with the Creem-issued key, passed as the ``api-key`` query
  parameter.

The two credentials are separate and cannot be substituted for each other:
presenting a main API key to the vault dataset endpoints is answered with
``HTTP 403 {"message": "Invalid API key"}``.

Because the authentication, the hosting and the commercial terms all differ, the
vault downloads live in this dedicated client instead of on
:py:class:`tradingstrategy.client.Client`.

Example:

.. code-block:: python

    from tradingstrategy.vault_data_client import VaultDataClient

    # Reads the licence key from the VAULT_PRO_API_KEY environment variable
    client = VaultDataClient()

    vault_universe = client.fetch_vault_universe()
    prices_df = client.fetch_vault_price_history()

See https://tradingstrategy.ai/vaults/datasets for the dataset catalogue and to
purchase a key.
"""

import datetime
import enum
import logging
import os
from email.utils import parsedate_to_datetime
from pathlib import Path

from collections.abc import Callable
from typing import TYPE_CHECKING

import orjson
import pandas as pd
import requests

from tradingstrategy.transport.cache_utils import wait_other_writers
from tradingstrategy.transport.progress_enabled_download import download_with_tqdm_progress_bar
from tradingstrategy.utils.time import naive_utcnow, naive_utcfromtimestamp

if TYPE_CHECKING:
    from tradingstrategy.vault import VaultUniverse

logger = logging.getLogger(__name__)


#: Base URL of the Cloudflare Worker serving the vault datasets.
#:
#: Each dataset is a path segment under this URL, see :py:class:`VaultDataset`.
VAULT_DATASETS_API_URL = "https://tradingstrategy.ai/vaults/datasets/download"

#: Environment variable holding the Creem licence key for vault datasets.
#:
#: This is the key emailed to the subscriber when they buy the Pro plan from
#: https://tradingstrategy.ai/vaults/datasets, formatted as five dash separated
#: groups. It is not :py:class:`tradingstrategy.client.Client`'s
#: ``TRADING_STRATEGY_API_KEY``, and it is not a ``creem_`` prefixed Creem
#: merchant API key, which belongs to the seller and is rejected here.
VAULT_PRO_API_KEY_ENV_VAR = "VAULT_PRO_API_KEY"

#: Root directory for downloaded vault datasets.
#:
#: Kept separate from the main dataset cache, so vault downloads can be
#: redirected on their own in tests and in strategy specific caches.
DEFAULT_VAULT_DOWNLOAD_ROOT = Path.home() / ".tradingstrategy" / "vaults" / "downloads"

#: How long a downloaded dataset is used before we revalidate it against the server.
DEFAULT_CACHE_EXPIRY = datetime.timedelta(hours=24)

#: Requests (connect, read) timeout for dataset downloads.
#:
#: The read timeout is generous, because the full price history is hundreds of megabytes.
DEFAULT_TIMEOUT = (15.0, 15 * 60.0)


class VaultDataAccessDenied(Exception):
    """The Creem API rejected our key.

    Raised for ``401`` and ``403`` answers, which mean the key is missing,
    invalid, expired, or not subscribed to the dataset being downloaded.
    """


class VaultDataset(enum.Enum):
    """Datasets downloadable from the vault dataset API.

    See https://tradingstrategy.ai/vaults/datasets for their contents and
    for which subscription tier covers each one.
    """

    #: Vault metadata: names, protocols, fees and lifetime performance metrics.
    vault_metadata = "vault_metadata"

    #: Vault share price and TVL history.
    vault_prices = "vault_prices"

    #: Metadata for cryptocurrency denominated vaults.
    crypto_metadata = "crypto_metadata"

    #: Cleaned cryptocurrency denominated price history.
    crypto_cleaned_prices = "crypto_cleaned_prices"

    #: Historical exchange rates used to denominate vault returns.
    exchange_rates = "exchange_rates"

    @property
    def endpoint(self) -> str:
        """URL path segment of this dataset.

        The API spells dataset names with dashes, we spell enum values with
        underscores, so translate here instead of storing unidiomatic values.
        """
        return self.value.replace("_", "-")

    @property
    def file_name(self) -> str:
        """Name of the locally cached file for this dataset."""
        extension = "json" if self in (VaultDataset.vault_metadata, VaultDataset.crypto_metadata) else "parquet"
        return f"{self.endpoint}.{extension}"


class VaultDataClient:
    """Download vault datasets from the Creem API.

    Handles authentication, caching and cache revalidation. Downloaded datasets
    are cached under :py:data:`DEFAULT_VAULT_DOWNLOAD_ROOT` and reused for
    :py:data:`DEFAULT_CACHE_EXPIRY`. After that the client asks the server
    whether the file changed, using a ``HEAD`` request, and only downloads again
    when it did. Answers are recorded in a sidecar JSON file next to the dataset,
    because a large price history should not be re-downloaded on every process
    start.

    See the module docstring for how this client relates to
    :py:class:`tradingstrategy.client.Client`.
    """

    def __init__(
        self,
        api_key: str | None = None,
        download_root: str | Path | None = None,
        base_url: str = VAULT_DATASETS_API_URL,
        session: requests.Session | None = None,
        timeout: float | tuple[float, float] = DEFAULT_TIMEOUT,
        cache_expiry: datetime.timedelta = DEFAULT_CACHE_EXPIRY,
        download_func: Callable = download_with_tqdm_progress_bar,
    ):
        """
        :param api_key:
            Creem API key for vault datasets.

            Read from the :py:data:`VAULT_PRO_API_KEY_ENV_VAR` environment
            variable when not given.

        :param download_root:
            Directory for cached datasets.

        :param base_url:
            Root URL of the dataset download API. Override in tests.

        :param session:
            Requests session to use, so callers can install their own retry
            adapters or record traffic in tests.

        :param timeout:
            Requests-style (connect, read) timeout.

        :param cache_expiry:
            How long a downloaded dataset is used before it is revalidated
            against the server.

        :param download_func:
            Function performing the actual HTTP download.

            Defaults to a downloader that renders a progress bar, because the
            datasets are large enough that a silent multi-minute stall would
            look like a hang.
        """

        if api_key is None:
            api_key = os.environ.get(VAULT_PRO_API_KEY_ENV_VAR)

        assert api_key, f"Vault datasets need a Creem API key. Pass api_key or set {VAULT_PRO_API_KEY_ENV_VAR}. See https://tradingstrategy.ai/vaults/datasets"

        self.api_key = api_key
        self.download_root = Path(download_root) if download_root is not None else DEFAULT_VAULT_DOWNLOAD_ROOT
        self.base_url = base_url.rstrip("/")
        self.session = session if session is not None else requests.Session()
        self.timeout = timeout
        self.cache_expiry = cache_expiry
        self.download_func = download_func

    def __repr__(self) -> str:
        # Never render the API key
        return f"<VaultDataClient {self.base_url}, cache {self.download_root}>"

    def get_url(self, dataset: VaultDataset) -> str:
        """Download URL of a dataset, without the API key.

        The key is passed as a request parameter rather than baked into this
        URL, so that logs and exceptions can quote the URL without leaking the
        credential.
        """
        return f"{self.base_url}/{dataset.endpoint}"

    def get_cached_path(self, dataset: VaultDataset) -> Path:
        """Local path of a dataset, whether or not it has been downloaded yet."""
        return self.download_root / dataset.file_name

    def download(self, dataset: VaultDataset) -> Path:
        """Download a dataset, or return the cached copy.

        :return:
            Path to the local dataset file.

        :raise VaultDataAccessDenied:
            If our API key is not accepted for this dataset.
        """

        assert isinstance(dataset, VaultDataset), f"Not a VaultDataset: {dataset}"

        url = self.get_url(dataset)
        path = self.get_cached_path(dataset)

        with wait_other_writers(path):
            remote_metadata = None

            if path.exists():
                cache_age = naive_utcnow() - naive_utcfromtimestamp(path.stat().st_mtime)
                local_size = path.stat().st_size

                if cache_age < self.cache_expiry:
                    logger.info(
                        "Vault dataset %s cache hit: path=%s, cache_age=%s, local_size=%d bytes",
                        dataset.value, path, cache_age, local_size,
                    )
                    return path

                logger.info(
                    "Vault dataset %s cache expired: path=%s, cache_age=%s, local_size=%d bytes. Revalidating against %s.",
                    dataset.value, path, cache_age, local_size, url,
                )

                remote_metadata = self._fetch_remote_metadata(url)
                if remote_metadata is not None and self._is_cache_current(path, remote_metadata):
                    self._store_cache_metadata(path, remote_metadata)
                    # Restart the expiry window, so an unchanged dataset is not
                    # revalidated on every call
                    os.utime(path, None)
                    logger.info("Reusing cached vault dataset %s: the server copy is unchanged", path)
                    return path
            else:
                logger.info("Vault dataset %s cache miss: downloading from %s", dataset.value, url)

            self.download_root.mkdir(parents=True, exist_ok=True)
            self._download_to(path, url, dataset)

            if remote_metadata is not None:
                self._store_cache_metadata(path, remote_metadata)

            return path

    def fetch_vault_universe(self) -> "VaultUniverse":
        """Download vault metadata and load it as a vault universe.

        Example:

        .. code-block:: python

            from tradingstrategy.chain import ChainId
            from tradingstrategy.vault_data_client import VaultDataClient

            client = VaultDataClient()
            vault_universe = client.fetch_vault_universe()

            vault = vault_universe.get_by_chain_and_name(ChainId.base, "IPOR USDC")
            print(f"TVL: ${vault.metadata.tvl:,.0f}")

        :return:
            Vault universe with full metadata.
        """

        from tradingstrategy.alternative_data.vault import load_vault_database_with_metadata

        path = self.download(VaultDataset.vault_metadata)
        data = path.read_bytes()
        try:
            return load_vault_database_with_metadata(orjson.loads(data))
        except orjson.JSONDecodeError as e:
            display_data = data.decode("utf-8", errors="replace")
            raise RuntimeError(f"Could not read vault metadata JSON file {path}\nData is {display_data}") from e

    def fetch_vault_price_history(self) -> pd.DataFrame:
        """Download vault share price history.

        :return:
            Price history with an explicit ``timestamp`` column, see
            :py:func:`normalise_vault_price_history_frame`.
        """

        path = self.download(VaultDataset.vault_prices)
        return normalise_vault_price_history_frame(pd.read_parquet(path))

    def _download_to(self, path: Path, url: str, dataset: VaultDataset) -> None:
        """Fetch a dataset over HTTP, showing a progress bar."""

        try:
            self.download_func(
                self.session,
                str(path),
                url,
                {"api-key": self.api_key},
                self.timeout,
                f"Downloading vault dataset {dataset.value}",
            )
        except Exception as e:
            # The downloader raises a generic error for any non-200 answer. Rejected credentials are the failure an operator
            # is most likely to hit and the least obvious from a stack trace,
            # so check for them separately and say what to do about it.
            self._raise_for_access(url, e)
            raise

    def _raise_for_access(self, url: str, cause: Exception) -> None:
        """Translate a rejected key into an actionable error."""

        response = self.session.get(
            url,
            params={"api-key": self.api_key},
            stream=True,
            timeout=self.timeout,
        )
        try:
            if response.status_code in (401, 403):
                raise VaultDataAccessDenied(
                    f"The vault dataset API rejected our Creem API key with HTTP {response.status_code} for {url}.\n"
                    f"Server says: {response.text[:200]}\n"
                    f"Check {VAULT_PRO_API_KEY_ENV_VAR}. Note the main Trading Strategy API key does not work here, "
                    f"vault datasets need their own key from https://tradingstrategy.ai/vaults/datasets"
                ) from cause
        finally:
            response.close()

    def _fetch_remote_metadata(self, url: str) -> dict | None:
        """Ask the server for the current cache headers of a dataset.

        :return:
            ``last_modified``, ``etag`` and ``content_length`` of the server
            copy, or ``None`` if the server could not be reached. A failed
            revalidation must not fail the download: we fall back to fetching
            the dataset again.
        """

        try:
            response = self.session.head(
                url,
                params={"api-key": self.api_key},
                allow_redirects=True,
                timeout=self.timeout,
            )
            if response.status_code in (401, 403):
                raise VaultDataAccessDenied(
                    f"The vault dataset API rejected our Creem API key with HTTP {response.status_code} for {url}.\n"
                    f"Check {VAULT_PRO_API_KEY_ENV_VAR}, see https://tradingstrategy.ai/vaults/datasets"
                )
            response.raise_for_status()
        except VaultDataAccessDenied:
            raise
        except Exception as e:
            logger.warning("Could not revalidate the vault dataset cache with a HEAD request to %s: %s", url, e)
            return None

        content_length = response.headers.get("Content-Length")
        if content_length is not None:
            try:
                content_length = int(content_length)
            except ValueError:
                logger.warning("Invalid Content-Length %r received from %s", content_length, url)
                content_length = None

        return {
            "last_modified": _parse_http_last_modified(response.headers.get("Last-Modified")),
            "etag": response.headers.get("ETag"),
            "content_length": content_length,
        }

    def _get_cache_metadata_path(self, path: Path) -> Path:
        """Sidecar file recording what the server said about a downloaded dataset."""
        return path.with_name(f"{path.name}.metadata.json")

    def _load_cache_metadata(self, path: Path) -> dict:
        """Read the sidecar metadata of a downloaded dataset."""
        metadata_path = self._get_cache_metadata_path(path)
        if not metadata_path.exists():
            return {}

        try:
            return orjson.loads(metadata_path.read_bytes())
        except Exception as e:
            logger.warning("Could not read cache metadata from %s: %s", metadata_path, e)
            return {}

    def _store_cache_metadata(self, path: Path, remote_metadata: dict) -> None:
        """Record what the server said, for the next revalidation."""
        last_modified = remote_metadata["last_modified"]
        payload = {
            "last_modified": last_modified.isoformat() if last_modified is not None else None,
            "etag": remote_metadata["etag"],
            "content_length": remote_metadata["content_length"],
            "stored_at": naive_utcnow().isoformat(),
        }
        self._get_cache_metadata_path(path).write_bytes(orjson.dumps(payload))

    def _is_cache_current(self, path: Path, remote_metadata: dict) -> bool:
        """Decide whether our cached copy still matches the server copy."""

        local_stat = path.stat()

        content_length = remote_metadata["content_length"]
        if content_length is not None and content_length != local_stat.st_size:
            return False

        local_metadata = self._load_cache_metadata(path)

        etag = remote_metadata["etag"]
        if etag and local_metadata.get("etag") == etag:
            return True

        last_modified = remote_metadata["last_modified"]
        if last_modified is None:
            return False

        stored_last_modified = local_metadata.get("last_modified")
        if stored_last_modified:
            try:
                if last_modified == datetime.datetime.fromisoformat(stored_last_modified):
                    return True
            except ValueError:
                logger.warning("Invalid cached last_modified value %r in %s", stored_last_modified, path)

        # No usable sidecar metadata, for example because the file was
        # downloaded by an older version. Fall back to file modification time.
        return last_modified <= naive_utcfromtimestamp(local_stat.st_mtime)


def _parse_http_last_modified(header_value: str | None) -> datetime.datetime | None:
    """Parse an HTTP ``Last-Modified`` header into a naive UTC datetime."""

    if not header_value:
        return None

    parsed = parsedate_to_datetime(header_value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return parsed.replace(tzinfo=None)


def normalise_vault_price_history_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise vault price history into a stable tabular shape.

    Why this exists:

    The price history parquet is produced by a separate data pipeline and, at
    the time of writing, stores ``timestamp`` in the parquet index. When pandas
    reads that parquet back we do *not* get a normal ``timestamp`` column,
    we get a ``DatetimeIndex`` named ``timestamp``.

    That shape is awkward for the rest of the codebase. Downstream loaders and
    strategy code work with datasets as plain tables and expect to be able to do
    ``df["timestamp"] >= some_date`` without caring how the parquet happened to
    encode its index.

    We fix the shape here, at the client boundary, because this is the layer
    that should hide transport and serialisation quirks from callers. Otherwise
    every caller would need to support both representations.
    """

    if "timestamp" not in df.columns:
        if "timestamp" in df.index.names:
            df = df.reset_index()
        elif isinstance(df.index, pd.DatetimeIndex):
            # Be defensive: even if the upstream writer drops the index name,
            # callers should still see the same schema.
            df = df.reset_index()
            first_column = df.columns[0]
            if first_column != "timestamp":
                df = df.rename(columns={first_column: "timestamp"})

    if "timestamp" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df
