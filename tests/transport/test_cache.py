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
            "candles-jsonl-1m-between-2021-07-01_14-35-17-and-any-b445ce65b8b8117fef6ee7b9b44b98ac.parquet"
        ),
        (
            {"pair_ids": {22, 1717, 88}},
            "candles-jsonl-1m-between-2021-07-01_14-35-17-and-any-3650b4fd16270e6522eddbeffa6fa676.parquet"
        ),
        (
            {"start_time": dt.datetime(2021, 11, 15, 19, 7, 50, 654321)},
            "candles-jsonl-1m-between-2021-11-15_19-07-50-and-any-932cd917d5f82f60c3c1e39c6725a427.parquet"
        ),
        (
            {"max_bytes": 1024 * 1024},
            "candles-jsonl-1m-between-2021-07-01_14-35-17-and-any-58000fef3f0a6af9d8393f50d530d3db.parquet"
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
            "candles-jsonl-1m-between-any-and-2022-08-18_12-59-00-3d66bc1d7a463e6ddb99fbc57e83b98e.parquet"
        ),
        (
            TimeBucket.m15,
            dt.datetime(2022, 8, 18, 12, 59, 59, 999999),
            "candles-jsonl-15m-between-any-and-2022-08-18_12-59-00-42b45ce03eb586963e1e660f8bd7cab8.parquet"
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


@pytest.mark.parametrize(
    "time_bucket, end_time, expected_name",
    (
        # For sub-day time buckets the minutes and seconds parts should be truncated
        # from end date.
        (
            TimeBucket.h1,
            dt.datetime(2022, 8, 18, 12, 34, 56, 987654),
            "candles-jsonl-1h-between-any-and-2022-08-18_12-00-00-e0c2fcf28f4b3a68761bcad0ba4e3e0e.parquet"
        ),
        (
            TimeBucket.h4,
            dt.datetime(2022, 8, 18, 12, 59, 59, 999999),
            "candles-jsonl-4h-between-any-and-2022-08-18_12-00-00-01dc13144bc9902e795b62eff9d17ed2.parquet"
        ),
    ),
)
def test__generate_cache_name_end_time_hour_precision(
    transport, time_bucket, end_time, expected_name
):
    name = transport._generate_cache_name(
        pair_ids=[10, 20, 30],
        end_time=end_time,
        time_bucket=time_bucket,
    )

    assert name == expected_name


@pytest.mark.parametrize(
    "time_bucket, end_time, expected_name",
    (
        # For time buckets longer than a day, the hours, minutes and seconds parts
        # should be truncated from end date.
        (
            TimeBucket.d1,
            dt.datetime(2022, 8, 18, 9, 5, 11, 111222),
            "candles-jsonl-1d-between-any-and-2022-08-18_00-00-00-c57f41e7e5bc1a97a2b09d1f326e4e27.parquet"
        ),
        (
            TimeBucket.d7,
            dt.datetime(2022, 8, 18, 12, 34, 56, 987654),
            "candles-jsonl-7d-between-any-and-2022-08-18_00-00-00-dd6d1182e6075c6c4ce1bebf1375fcae.parquet"
        ),
        (
            TimeBucket.d30,
            dt.datetime(2022, 8, 18, 23, 59, 59, 999999),
            "candles-jsonl-30d-between-any-and-2022-08-18_00-00-00-977074bcdc3cc020db20871d944ca183.parquet"
        ),
        # We omit d360, because yearly candles do not make much sense in trading and nobody
        # uses them.
    ),
)
def test__generate_cache_name_end_time_day_precision(
    transport, time_bucket, end_time, expected_name
):
    name = transport._generate_cache_name(
        pair_ids=[10, 20, 30],
        end_time=end_time,
        time_bucket=time_bucket,
    )

    assert name == expected_name
