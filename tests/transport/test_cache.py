import datetime as dt
import time
from unittest.mock import Mock

import pytest

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

