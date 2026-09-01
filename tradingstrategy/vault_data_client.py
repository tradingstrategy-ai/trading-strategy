"""Vault dataset downloads through the Creem API.

Vault metadata and vault share price history are commercial datasets. They are
**not** served by the main Trading Strategy oracle API, and they do not use the
main API key.

The datasets are sold as a monthly licence through `Creem <https://creem.io>`__
and served by a Cloudflare-fronted download API. Authentication is a licence
key passed as the ``api-key`` query parameter. The key is emailed to the
subscriber on purchase and looks like ``XXXXX-XXXXX-XXXXX-XXXXX-XXXXX``.

Three similar looking credentials are easy to confuse, and only the first one
works here. The other two are answered with ``HTTP 403 {"message": "Invalid API
key"}``:

- the **licence key** emailed to the subscriber, five dash separated groups;
- the **main oracle API key**, which is ``secret-token:`` prefixed and sent in
  the ``Authorization`` header by :py:class:`tradingstrategy.client.Client`.
  Note the website's ``curl`` example stores the licence key in a shell
  variable called ``TRADING_STRATEGY_API_KEY``, which is *not* that key;
- a ``creem_`` prefixed **Creem merchant API key**, which belongs to the seller
  of the dataset, authenticates against ``api.creem.io`` and must never be sent
  to a download endpoint.

Because the authentication, the hosting and the commercial terms all differ, the
vault downloads live in this dedicated client instead of on
:py:class:`tradingstrategy.client.Client`.

A limited free sample of the vault datasets, covering Ethereum only, is
published without any key at
``https://tradingstrategy.ai/vaults/datasets/sample/``. This client does not
download it, because a strategy silently backtesting on a one chain sample is
worse than a strategy failing to start.

Example:

.. code-block:: python

    from tradingstrategy.vault_data_client import VaultDataClient

    # Reads the licence key from the VAULT_PRO_API_KEY environment variable
    client = VaultDataClient()

    vault_universe = client.fetch_vault_universe()
    prices_df = client.fetch_vault_price_history()

See https://tradingstrategy.ai/vaults/datasets for the dataset catalogue and to
buy a licence.
"""
import datetime
import enum
import logging
import os
from pathlib import Path

from collections.abc import Callable
from typing import TYPE_CHECKING

import orjson
import pandas as pd
import requests

from tradingstrategy.alternative_data.vault import load_vault_database_with_metadata
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

#: How long a downloaded dataset is served from the local cache.
#:
#: TODO: The download API currently answers with ``Content-Length`` only. It
#: sends no ``ETag`` and no ``Last-Modified``, and marks responses
#: ``cache-control: private, no-store``, so there is no validator to revalidate
#: an expired cache against. Until the API exposes one, a plain local time to
#: live is the whole cache policy: after it elapses the dataset is downloaded
#: again. Size alone was considered and rejected, because a rewritten dataset
#: can keep the same length and would then never be refreshed.
DEFAULT_CACHE_EXPIRY = datetime.timedelta(hours=12)

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

    #: Vault identity, chain, protocol, fees, status and lifetime performance
    #: metrics, as JSON. Served as ``vault-metadata.json``.
    vault_metadata = "vault_metadata"

    #: Hourly USD share price and TVL history, as Parquet. Served as
    #: ``vault-historical.parquet``. This is the dataset that replaced the old
    #: public ``cleaned-vault-prices-1h.parquet`` bucket object.
    vault_prices = "vault_prices"

    #: Metadata for vaults denominated in cryptocurrency rather than a
    #: stablecoin, as JSON. Served as ``crypto-vault-metadata.json``.
    crypto_metadata = "crypto_metadata"

    #: Daily cleaned price history for cryptocurrency denominated vaults, as
    #: Parquet. Served as ``crypto-cleaned-vault-prices-1d.parquet``. Note the
    #: daily frequency, unlike the hourly :py:attr:`vault_prices`.
    crypto_cleaned_prices = "crypto_cleaned_prices"

    #: Historical exchange rates used to denominate vault returns, as Parquet.
    #: Served as ``exchange-rates.parquet``.
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
        """Name of the locally cached file for this dataset.

        Named after the dataset rather than after the file the server happens to
        send, so that the cache directory stays predictable if the published
        file names change.
        """
        extension = "json" if self in (VaultDataset.vault_metadata, VaultDataset.crypto_metadata) else "parquet"
        return f"{self.endpoint}.{extension}"


class VaultDataClient:
    """Download vault datasets from the Creem API.

    Handles authentication and caching. Datasets are cached under
    :py:data:`DEFAULT_VAULT_DOWNLOAD_ROOT` and served from there for
    :py:data:`DEFAULT_CACHE_EXPIRY` without contacting the server, then
    downloaded again. See :py:data:`DEFAULT_CACHE_EXPIRY` for why there is no
    cheaper revalidation than a full download.

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
            How long a downloaded dataset is served from the local cache before
            it is downloaded again.

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
        # Resolved, because the cache lock refuses relative paths and a caller
        # passing an ordinary project relative directory should not hit that
        # assertion halfway through a download
        root = Path(download_root) if download_root is not None else DEFAULT_VAULT_DOWNLOAD_ROOT
        self.download_root = root.expanduser().resolve()
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
            If our licence key is not accepted for this dataset.
        """

        assert isinstance(dataset, VaultDataset), f"Not a VaultDataset: {dataset}"

        path = self.get_cached_path(dataset)

        with wait_other_writers(path):
            if self._is_cache_fresh(path, dataset):
                return path

            self._download_to(path, self.get_url(dataset), dataset)
            return path

    def _is_cache_fresh(self, path: Path, dataset: VaultDataset) -> bool:
        """Is the locally cached dataset young enough to use?"""

        if not path.exists():
            logger.info("Vault dataset %s cache miss: no file at %s", dataset.value, path)
            return False

        stat = path.stat()
        cache_age = naive_utcnow() - naive_utcfromtimestamp(stat.st_mtime)
        if cache_age >= self.cache_expiry:
            logger.info(
                "Vault dataset %s cache expired: path=%s, cache_age=%s, local_size=%d bytes",
                dataset.value, path, cache_age, stat.st_size,
            )
            return False

        logger.info(
            "Vault dataset %s cache hit: path=%s, cache_age=%s, local_size=%d bytes",
            dataset.value, path, cache_age, stat.st_size,
        )
        return True

    def _download_to(self, path: Path, url: str, dataset: VaultDataset) -> None:
        """Stream a dataset to disk, replacing the cached copy only once complete.

        The download goes to a temporary file next to the cache entry and is
        moved into place with :py:func:`os.replace`. Writing straight to the
        cache path would let a connection failure destroy a good copy and leave
        a truncated file behind, which the next call would then serve as a fresh
        cache hit.
        """

        self.download_root.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f"{path.name}.part")

        logger.info("Downloading vault dataset %s from %s to %s", dataset.value, url, path)
        try:
            self.download_func(
                self.session,
                str(temp_path),
                url,
                {"api-key": self.api_key},
                self.timeout,
                f"Downloading vault dataset {dataset.value}",
            )
        except Exception as e:
            temp_path.unlink(missing_ok=True)

            # The downloader reports any non-200 answer as a generic error.
            # If the licence key is the problem, say so instead.
            self._raise_if_access_denied(url)

            raise RuntimeError(f"Could not download vault dataset {dataset.value} from {url}: {self._redact(e)}") from None

        os.replace(temp_path, path)

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

    def _raise_if_access_denied(self, url: str) -> None:
        """Turn a rejected licence key into an actionable error.

        Called after a failed download, because the downloader reports every
        non-200 answer the same way and a wrong key is both the most likely
        cause and the least obvious one from a stack trace.

        :raise VaultDataAccessDenied:
            If the server rejects our licence key.
        """

        # Only the request itself is guarded. Deciding what the answer means
        # must not be, or a rejected key could be downgraded to a warning by the
        # same handler that tolerates a flaky network.
        try:
            response = self.session.head(
                url,
                params={"api-key": self.api_key},
                allow_redirects=True,
                timeout=self.timeout,
            )
        except Exception as e:
            logger.warning("Could not check vault dataset access with a HEAD request to %s: %s", url, self._redact(e))
            return

        if response.status_code in (401, 403):
            raise VaultDataAccessDenied(
                f"The vault dataset API rejected our licence key with HTTP {response.status_code} for {url}.\n"
                f"Check {VAULT_PRO_API_KEY_ENV_VAR}. It must hold the licence key emailed on purchase, "
                f"formatted as five dash separated groups. Neither the main Trading Strategy API key nor a "
                f"creem_ prefixed Creem merchant key works here. "
                f"See https://tradingstrategy.ai/vaults/datasets"
            )

    def _redact(self, value: object) -> str:
        """Describe a value without disclosing the licence key.

        The key travels as a URL query parameter, so ``requests`` builds it into
        the prepared URL and any connection, redirect or status error it raises
        quotes that URL back. Those messages reach logs and tracebacks, so
        everything derived from an exception passes through here first.
        """
        return str(value).replace(self.api_key, "***")


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
