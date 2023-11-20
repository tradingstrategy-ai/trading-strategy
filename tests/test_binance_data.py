import datetime
import pytest
import pandas as pd
from pathlib import Path

from tradingstrategy.binance_data import BinanceDownloader
from tradingstrategy.timebucket import TimeBucket


CANDLE_SYMBOL = "ETHUSDC"
LENDING_SYMBOL = "ETH"
LENDING_TIME_BUCKET = TimeBucket.d1
TIME_BUCKET = TimeBucket.h1
START_AT = datetime.datetime(2021, 1, 1)
END_AT = datetime.datetime(2021, 1, 2)


@pytest.fixture(scope="module")
def candle_downloader():
    return BinanceDownloader()


def test_read_fresh_candle_data(candle_downloader: BinanceDownloader):
    """Test reading fresh candle data. Will be downloaded from Binance API."""
    df = candle_downloader.fetch_candlestick_data(
        CANDLE_SYMBOL,
        TIME_BUCKET,
        START_AT,
        END_AT,
        force_redownload=True,
    )

    assert len(df) == 49
    assert df.columns.tolist() == ["open", "high", "low", "close", "volume"]
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False


def test_read_cached_candle_data(candle_downloader: BinanceDownloader):
    """Test reading cached candle data. Must be run after test_read_fresh_candle_data."""
    df = candle_downloader.get_data_parquet(
        CANDLE_SYMBOL, TIME_BUCKET, START_AT, END_AT
    )

    assert len(df) == 49
    assert df.columns.tolist() == ["open", "high", "low", "close", "volume"]
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False


def test_read_fresh_lending_data(candle_downloader: BinanceDownloader):
    """Test reading fresh lending data. Will be downloaded from Binance API."""
    series = candle_downloader.fetch_lending_rates(
        LENDING_SYMBOL,
        LENDING_TIME_BUCKET,
        START_AT,
        END_AT,
        force_redownload=True,
    )

    assert len(series) == 2
    assert series.isna().sum() == 0
    assert series.isna().values.any() == False


def test_read_cached_lending_data(candle_downloader: BinanceDownloader):
    """Test reading cached candle data. Must be run after test_read_fresh_lending_data."""
    series = candle_downloader.get_data_parquet(
        LENDING_SYMBOL, LENDING_TIME_BUCKET, START_AT, END_AT, is_lending=True
    )

    assert len(series) == 2
    assert series.isna().sum().sum() == 0
    assert series.isna().values.any() == False


def test_purge_cache(candle_downloader: BinanceDownloader):
    """Test purging cached candle data. Must be run after test_read_fresh_candle_data and test_read_cached_candle_data"""
    candle_path = candle_downloader.get_parquet_path(CANDLE_SYMBOL, TIME_BUCKET, START_AT, END_AT)
    assert candle_path.exists() == True
    candle_downloader.purge_cached_file(path=candle_path)
    assert candle_path.exists() == False

    lending_path = candle_downloader.get_parquet_path(LENDING_SYMBOL, LENDING_TIME_BUCKET, START_AT, END_AT, is_lending=True)
    assert lending_path.exists() == True
    candle_downloader.purge_cached_file(path=lending_path)
    assert lending_path.exists() == False

    candle_downloader.purge_all_cached_data()
    assert len(list(candle_downloader.cache_directory.iterdir())) == 0
