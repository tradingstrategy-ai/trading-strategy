import datetime as dt
import os
import time
from email.utils import format_datetime
from pathlib import Path
from unittest.mock import Mock

import orjson
import pytest

from tradingstrategy.transport.cache import CachedHTTPTransport
from tradingstrategy.timebucket import TimeBucket


@pytest.fixture()
def transport():
    from tradingstrategy.transport.cache import CachedHTTPTransport

    transport = CachedHTTPTransport(download_func=Mock())
    yield transport
    transport.close()


def test_get_cached_item_no_cache_file(transport, tmp_path):
    filename = transport.get_cached_item(tmp_path / "not_here.parquet")
    assert filename is None


def test_get_cached_item_cache_file_with_end_time(transport, tmp_path):
    # Cached candle data with end time should never expire, no matter how old they are
    tmp_file = tmp_path / "candles-jsonl-1m-to_1998-03-31_19-38-02-58000fef3f0a6af9d8393f50d530d3db.parquet"
    tmp_file.write_text("I am candle data!")  # actually create the file

    transport.cache_period = dt.timedelta(microseconds=1)  # Should NOT affect expiration
    time.sleep(0.000_002) # Assure that cache period otherwise definitely expires
    filename = transport.get_cached_item(tmp_file)

    assert filename == tmp_file


def test_get_cached_item_cache_file_no_end_time_recent_enough(transport, tmp_path):
    # Cached candle data with end time should never expire, no matter how old they are
    tmp_file = tmp_path / "candles-jsonl-1m-58000fef3f0a6af9d8393f50d530d3db.parquet"
    tmp_file.write_text("I am candle data!")  # actually create the file

    transport.cache_period = dt.timedelta(days=1)
    filename = transport.get_cached_item(tmp_file)

    assert filename == tmp_file


def test_get_cached_item_cache_file_no_end_time_expired(transport, tmp_path):
    # Cached candle data with end time should never expire, no matter how old they are
    tmp_file = tmp_path / "candles-jsonl-1m-58000fef3f0a6af9d8393f50d530d3db.parquet"
    tmp_file.write_text("I am candle data!")  # actually create the file

    transport.cache_period = dt.timedelta(microseconds=1)
    time.sleep(0.000_002)  # Assure cache expires even on the fastest computer in the universe
    filename = transport.get_cached_item(tmp_file)

    assert filename is None


@pytest.mark.parametrize(
    "kwarg_overrides, expected_name",
    (
        (
            {},  # no overrides
            "candles-1m-between-2021-07-01_14-35-17-and-any-7c33c210096558933e5ba446e1d36bf2.parquet"
        ),
    ),
)
def test__generate_cache_name_no_end_time(transport, kwarg_overrides, expected_name):
    kwargs = {
        "pair_ids": {2, 17, 8},
        "time_bucket": TimeBucket.m1,
        "start_time": dt.datetime(2021, 7, 1, 14, 35, 17, 123456),
        "end_time": None,
        "max_bytes": 1337,
    } | kwarg_overrides

    name = transport._generate_cache_name(**kwargs)

    assert name == expected_name


@pytest.mark.parametrize(
    "time_bucket, end_time, expected_name",
    (
        # For sub-hour time buckets the seconds part should be truncated from end date.
        (
            TimeBucket.m1,
            dt.datetime(2022, 8, 18, 12, 59, 24, 987654),
            "candles-1m-between-any-and-2022-08-18_12-59-00-d9a6ebc5ed451d6669ec0076e65e496d.parquet"
        ),
        (
            TimeBucket.m15,
            dt.datetime(2022, 8, 18, 12, 59, 59, 999999),
            "candles-15m-between-any-and-2022-08-18_12-59-00-1798a5e00d5de2666781f50d825ec9af.parquet"
        ),
    ),
)
def test__generate_cache_name_end_time_minute_precision(
    transport, time_bucket, end_time, expected_name
):
    name = transport._generate_cache_name(
        pair_ids=[10, 20, 30],
        end_time=end_time,
        time_bucket=time_bucket,
    )

    assert name == expected_name


def test_fetch_vault_price_history_reuses_expired_cache_when_head_matches(
    transport: CachedHTTPTransport,
    tmp_path: Path,
) -> None:
    """Test expired vault parquet cache reuse when remote HEAD metadata is unchanged.

    1. Create an expired local parquet cache without any sidecar metadata yet.
    2. Mock a remote HEAD response whose Last-Modified and Content-Length still match the local file.
    3. Confirm the transport skips the download and refreshes the local cache timestamp.
    """
    download_root = tmp_path / "vault-downloads"
    download_root.mkdir()
    cached_path = download_root / "vault-price-history.parquet"
    cached_path.write_bytes(b"abc")

    expired_mtime = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=2)
    os.utime(cached_path, (expired_mtime.timestamp(), expired_mtime.timestamp()))

    remote_last_modified = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=3)
    head_response = Mock()
    head_response.headers = {
        "Last-Modified": format_datetime(remote_last_modified),
        "Content-Length": "3",
    }
    head_response.raise_for_status = Mock()
    transport.requests.head = Mock(return_value=head_response)

    # 1. Create an expired local parquet cache without any sidecar metadata yet.
    original_mtime = cached_path.stat().st_mtime

    # 2. Mock a remote HEAD response whose Last-Modified and Content-Length still match the local file.
    result = transport.fetch_vault_price_history(
        url="https://example.com/cleaned-vault-prices-1h.parquet",
        download_root=download_root,
    )

    # 3. Confirm the transport skips the download and refreshes the local cache timestamp.
    assert result == cached_path
    transport.download_func.assert_not_called()
    assert cached_path.stat().st_mtime > original_mtime
    sidecar_path = cached_path.with_name("vault-price-history.parquet.metadata.json")
    assert sidecar_path.exists()
    assert orjson.loads(sidecar_path.read_bytes())["content_length"] == 3


def test_fetch_vault_price_history_redownloads_when_head_metadata_changed(
    transport: CachedHTTPTransport,
    tmp_path: Path,
) -> None:
    """Test vault parquet redownload when remote HEAD metadata has changed.

    1. Create an expired local parquet cache to force remote validation.
    2. Mock a remote HEAD response whose Last-Modified and Content-Length indicate a newer file.
    3. Confirm the transport performs a fresh download and stores the new sidecar metadata.
    """
    download_root = tmp_path / "vault-downloads"
    download_root.mkdir()
    cached_path = download_root / "vault-price-history.parquet"
    cached_path.write_bytes(b"abc")

    expired_mtime = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=2)
    os.utime(cached_path, (expired_mtime.timestamp(), expired_mtime.timestamp()))

    remote_last_modified = dt.datetime.now(dt.timezone.utc)
    head_response = Mock()
    head_response.headers = {
        "Last-Modified": format_datetime(remote_last_modified),
        "Content-Length": "5",
        "ETag": '"new-version"',
    }
    head_response.raise_for_status = Mock()
    transport.requests.head = Mock(return_value=head_response)

    def fake_download(session, path, url, params, timeout, human_desc) -> None:
        Path(path).write_bytes(b"abcde")

    transport.download_func.side_effect = fake_download

    # 1. Create an expired local parquet cache to force remote validation.
    result = transport.fetch_vault_price_history(
        url="https://example.com/cleaned-vault-prices-1h.parquet",
        download_root=download_root,
    )

    # 2. Mock a remote HEAD response whose Last-Modified and Content-Length indicate a newer file.
    sidecar_path = cached_path.with_name("vault-price-history.parquet.metadata.json")

    # 3. Confirm the transport performs a fresh download and stores the new sidecar metadata.
    assert result == cached_path
    transport.download_func.assert_called_once()
    assert cached_path.read_bytes() == b"abcde"
    assert sidecar_path.exists()
    assert orjson.loads(sidecar_path.read_bytes())["etag"] == '"new-version"'
