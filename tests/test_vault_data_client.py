"""Tests for vault dataset downloads through the Creem API."""

import datetime
import os
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

from tradingstrategy.vault import VaultUniverse
from tradingstrategy.vault_data_client import (
    VaultDataAccessDenied,
    VaultDataClient,
    VaultDataset,
    VAULT_PRO_API_KEY_ENV_VAR,
    normalise_vault_price_history_frame,
)


@pytest.fixture()
def download_func() -> Mock:
    """Stand in for the HTTP downloader.

    The real downloader streams hundreds of megabytes from a paid API, so unit
    tests substitute it and assert on how it was called.
    """

    def write_dataset(session, path, url, params, timeout, human_readable_hint) -> None:
        Path(path).write_bytes(b"dataset")

    return Mock(side_effect=write_dataset)


@pytest.fixture()
def client(tmp_path: Path, download_func: Mock) -> VaultDataClient:
    """Vault dataset client writing to a throwaway cache directory."""
    return VaultDataClient(
        api_key="test-licence-key",
        download_root=tmp_path / "vault-downloads",
        session=Mock(),
        download_func=download_func,
    )


def expire_cache(path: Path) -> None:
    """Backdate a cached file so the client treats it as expired."""
    expired = path.stat().st_mtime - datetime.timedelta(days=2).total_seconds()
    os.utime(path, (expired, expired))


def test_expired_cache_is_downloaded_again(
    client: VaultDataClient,
    download_func: Mock,
) -> None:
    """Check an expired cache is replaced rather than served.

    The API offers no cache validator, so an expired dataset can only be
    refreshed by downloading it again.

    1. Download a dataset and then backdate it past the cache expiry.
    2. Download it again.
    3. Verify the dataset was fetched a second time.
    """

    # 1. Download a dataset and then backdate it past the cache expiry.
    path = client.download(VaultDataset.vault_prices)
    expire_cache(path)

    # 2. Download it again.
    result = client.download(VaultDataset.vault_prices)

    # 3. Verify the dataset was fetched a second time.
    assert result == path
    assert download_func.call_count == 2


def test_interrupted_download_keeps_the_previous_copy(
    client: VaultDataClient,
    download_func: Mock,
) -> None:
    """Check a failed download neither destroys nor replaces the cached dataset.

    Writing straight to the cache path would let a dropped connection leave a
    truncated file behind with a fresh timestamp, which the next call would
    then serve as a valid cache hit for the whole expiry window.

    1. Download a dataset and then backdate it past the cache expiry.
    2. Fail the next download part way through writing.
    3. Verify the previous copy is intact and no partial file was left behind.
    """

    # 1. Download a dataset and then backdate it past the cache expiry.
    path = client.download(VaultDataset.vault_prices)
    expire_cache(path)

    # 2. Fail the next download part way through writing.
    def fail_midway(session, download_path, url, params, timeout, human_readable_hint) -> None:
        Path(download_path).write_bytes(b"trunc")
        raise ConnectionError("Connection broken: IncompleteRead")

    download_func.side_effect = fail_midway
    client.session.head = Mock(side_effect=ConnectionError("no route to host"))

    with pytest.raises(RuntimeError):
        client.download(VaultDataset.vault_prices)

    # 3. Verify the previous copy is intact and no partial file was left behind.
    assert path.read_bytes() == b"dataset"
    assert list(path.parent.glob("*.part")) == []


def test_download_errors_do_not_disclose_the_licence_key(
    client: VaultDataClient,
    download_func: Mock,
) -> None:
    """Check the licence key never reaches an error message.

    The key travels as a URL query parameter, so ``requests`` builds it into
    the prepared URL and quotes that URL back in connection and status errors.

    1. Fail a download with an error quoting the full request URL.
    2. Verify the raised error describes the failure without the key.
    """

    # 1. Fail a download with an error quoting the full request URL.
    download_func.side_effect = ConnectionError(
        "HTTPSConnectionPool: /download/vault-prices?api-key=test-licence-key timed out"
    )
    client.session.head = Mock(side_effect=ConnectionError("also down"))

    # 2. Verify the raised error describes the failure without the key.
    with pytest.raises(RuntimeError) as raised:
        client.download(VaultDataset.vault_prices)

    message = str(raised.value)
    assert "test-licence-key" not in message
    assert "***" in message


def test_rejected_api_key_is_reported_clearly(client: VaultDataClient) -> None:
    """Check a refused licence key produces an actionable error.

    The main Trading Strategy API key does not work for vault datasets, and the
    server answers a wrong key with a bare ``403``. Without translation that
    surfaces as an opaque download failure.

    1. Fail the download the way the API answers a bad key.
    2. Answer the follow-up access check with the server's rejection.
    3. Verify the error names the environment variable to fix.
    """

    # 1. Fail the download the way the API answers a bad key.
    client.download_func.side_effect = RuntimeError("Failed to do an API call")

    # 2. Answer the follow-up access check with the server's rejection.
    forbidden = Mock()
    forbidden.status_code = 403
    forbidden.headers = {}
    client.session.head = Mock(return_value=forbidden)

    # 3. Verify the error names the environment variable to fix.
    with pytest.raises(VaultDataAccessDenied) as raised:
        client.download(VaultDataset.vault_metadata)

    message = str(raised.value)
    assert VAULT_PRO_API_KEY_ENV_VAR in message
    # The two credentials that are easy to reach for by mistake
    assert "creem_" in message


def test_missing_api_key_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """Check the client refuses to start without a Creem key.

    1. Remove the key from the environment.
    2. Verify constructing the client fails with a message pointing at the key.
    """

    # 1. Remove the key from the environment.
    monkeypatch.delenv(VAULT_PRO_API_KEY_ENV_VAR, raising=False)

    # 2. Verify constructing the client fails with a message pointing at the key.
    with pytest.raises(AssertionError) as raised:
        VaultDataClient()

    assert VAULT_PRO_API_KEY_ENV_VAR in str(raised.value)


def test_price_history_normalises_timestamp_column(tmp_path: Path) -> None:
    """Check price history always exposes ``timestamp`` as a column.

    The upstream pipeline stores the timestamp in the parquet index, which
    pandas reads back as a ``DatetimeIndex`` rather than a column. Callers
    should not have to handle both shapes.

    1. Write a parquet fixture where ``timestamp`` lives in the index.
    2. Normalise the frame read back from it.
    3. Verify ``timestamp`` is a regular column with the original value.
    """

    # 1. Write a parquet fixture where ``timestamp`` lives in the index.
    parquet_path = tmp_path / "vault-prices.parquet"
    pd.DataFrame(
        {
            "chain": [8453],
            "address": ["0x45aa96f0b3188d47a1dafdbefce1db6b37f58216"],
            "share_price": [1.01],
            "total_assets": [1000.0],
        },
        index=pd.DatetimeIndex([pd.Timestamp("2025-01-01 00:00:00")], name="timestamp"),
    ).to_parquet(parquet_path)

    # 2. Normalise the frame read back from it.
    result = normalise_vault_price_history_frame(pd.read_parquet(parquet_path))

    # 3. Verify ``timestamp`` is a regular column with the original value.
    assert "timestamp" in result.columns
    assert result.iloc[0]["timestamp"] == pd.Timestamp("2025-01-01 00:00:00")


@pytest.mark.skipif(
    os.environ.get(VAULT_PRO_API_KEY_ENV_VAR) is None,
    reason=f"Set {VAULT_PRO_API_KEY_ENV_VAR} environment variable to run this test",
)
def test_fetch_vault_datasets_live(tmp_path: Path) -> None:
    """Check both vault datasets can be downloaded and parsed from the live API.

    1. Create a client reading the Creem key from the environment.
    2. Download and parse vault metadata.
    3. Download and parse vault price history.
    """

    # 1. Create a client reading the Creem key from the environment.
    client = VaultDataClient(download_root=tmp_path / "vault-downloads")

    # 2. Download and parse vault metadata.
    vault_universe = client.fetch_vault_universe()
    assert isinstance(vault_universe, VaultUniverse)
    assert vault_universe.get_vault_count() > 0

    # 3. Download and parse vault price history.
    history_df = client.fetch_vault_price_history()
    assert len(history_df) > 0
    assert {"timestamp", "chain", "address", "share_price", "total_assets"}.issubset(history_df.columns)
    assert history_df["timestamp"].notna().all()
