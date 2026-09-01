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
    """Vault dataset client writing to a throwaway cache directory.

    The session answers the size check with an unknown size by default, so a
    test that does not care about revalidation always downloads.
    """
    session = Mock()
    session.head = Mock(return_value=make_head_response(None))
    return VaultDataClient(
        api_key="test-licence-key",
        download_root=tmp_path / "vault-downloads",
        session=session,
        download_func=download_func,
    )


def make_head_response(content_length: int | None) -> Mock:
    """Build a mocked HEAD answer describing the server copy of a dataset."""
    response = Mock()
    response.headers = {} if content_length is None else {"Content-Length": str(content_length)}
    response.status_code = 200
    response.ok = True
    return response


def expire_cache(path: Path) -> None:
    """Backdate a cached file so the client revalidates it against the server."""
    expired = (datetime.datetime.now() - datetime.timedelta(days=2)).timestamp()
    os.utime(path, (expired, expired))


def test_download_caches_dataset_and_sends_api_key(
    client: VaultDataClient,
    download_func: Mock,
) -> None:
    """Check a dataset is downloaded once and then served from the cache.

    1. Download a dataset into an empty cache.
    2. Verify the file landed on disk and the Creem key travelled as a request parameter.
    3. Download the same dataset again.
    4. Verify the cached copy was reused instead of downloading a second time.
    """

    # 1. Download a dataset into an empty cache.
    path = client.download(VaultDataset.vault_prices)

    # 2. Verify the file landed on disk and the Creem key travelled as a request parameter.
    assert path.exists()
    assert path.name == "vault-prices.parquet"
    download_func.assert_called_once()
    _, called_path, called_url, called_params, _, _ = download_func.call_args[0]
    assert called_path == str(path)
    assert called_url == "https://tradingstrategy.ai/vaults/datasets/download/vault-prices"
    assert called_params == {"api-key": "test-licence-key"}

    # 3. Download the same dataset again.
    second_path = client.download(VaultDataset.vault_prices)

    # 4. Verify the cached copy was reused instead of downloading a second time.
    assert second_path == path
    download_func.assert_called_once()


def test_expired_cache_is_reused_when_server_copy_is_unchanged(
    client: VaultDataClient,
    download_func: Mock,
) -> None:
    """Check an expired cache survives when the server copy is the same size.

    The price history is a few hundred megabytes, so an expired cache must not
    mean an automatic re-download. The client asks the server how large the
    dataset is and only downloads when the answer differs.

    1. Download a dataset and then backdate it past the cache expiry.
    2. Answer the size check with the size of the local file.
    3. Verify no new download happened and the expiry window restarted.
    """

    # 1. Download a dataset and then backdate it past the cache expiry.
    path = client.download(VaultDataset.vault_prices)
    expire_cache(path)
    original_mtime = path.stat().st_mtime

    # 2. Answer the size check with the size of the local file.
    client.session.head = Mock(return_value=make_head_response(path.stat().st_size))
    result = client.download(VaultDataset.vault_prices)

    # 3. Verify no new download happened and the expiry window restarted.
    assert result == path
    download_func.assert_called_once()
    assert path.stat().st_mtime > original_mtime


def test_expired_cache_is_redownloaded_when_server_copy_changed(
    client: VaultDataClient,
    download_func: Mock,
) -> None:
    """Check a differently sized server copy triggers a fresh download.

    1. Download a dataset and then backdate it past the cache expiry.
    2. Answer the size check with a larger dataset.
    3. Verify the dataset was downloaded again.
    """

    # 1. Download a dataset and then backdate it past the cache expiry.
    path = client.download(VaultDataset.vault_prices)
    expire_cache(path)

    # 2. Answer the size check with a larger dataset.
    client.session.head = Mock(return_value=make_head_response(path.stat().st_size + 100))
    result = client.download(VaultDataset.vault_prices)

    # 3. Verify the dataset was downloaded again.
    assert result == path
    assert download_func.call_count == 2


def test_expired_cache_is_redownloaded_when_size_is_unknown(
    client: VaultDataClient,
    download_func: Mock,
) -> None:
    """Check an unusable size answer falls back to downloading.

    The API sends no ETag and no Last-Modified, so ``Content-Length`` is the
    only validator. Without it there is nothing to compare and stale data is a
    worse outcome than a repeated download.

    1. Download a dataset and then backdate it past the cache expiry.
    2. Answer the size check without a Content-Length header.
    3. Verify the dataset was downloaded again.
    """

    # 1. Download a dataset and then backdate it past the cache expiry.
    path = client.download(VaultDataset.vault_prices)
    expire_cache(path)

    # 2. Answer the size check without a Content-Length header.
    client.session.head = Mock(return_value=make_head_response(None))
    client.download(VaultDataset.vault_prices)

    # 3. Verify the dataset was downloaded again.
    assert download_func.call_count == 2


def test_rejected_api_key_is_reported_clearly(client: VaultDataClient) -> None:
    """Check a refused licence key produces an actionable error.

    The main Trading Strategy API key does not work for vault datasets, and the
    server answers a wrong key with a bare ``403``. Without translation that
    surfaces as an opaque download failure.

    1. Download a dataset and then backdate it past the cache expiry.
    2. Answer the revalidation with the server's rejection.
    3. Verify the error names the environment variable to fix.
    """

    # 1. Download a dataset and then backdate it past the cache expiry.
    path = client.download(VaultDataset.vault_metadata)
    expire_cache(path)

    # 2. Answer the revalidation with the server's rejection.
    forbidden = Mock()
    forbidden.status_code = 403
    forbidden.headers = {}
    forbidden.ok = False
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
