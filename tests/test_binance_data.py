import datetime
import pytest
import pandas as pd
from pathlib import Path

from tradingstrategy.binance_data import BinanceDownloader
from tradingstrategy.timebucket import TimeBucket


SYMBOL = "ETHUSDC"
TIME_BUCKET = TimeBucket.h1
START_AT = datetime.datetime(2021, 1, 1)
END_AT = datetime.datetime(2021, 1, 2)


@pytest.fixture(scope="module")
def candle_downloader():
    return BinanceDownloader()


@pytest.fixture(scope="module")
def path(candle_downloader: BinanceDownloader):
    return candle_downloader.get_parquet_path(SYMBOL, TIME_BUCKET, START_AT, END_AT)


def test_read_fresh_candle_data(candle_downloader: BinanceDownloader):
    """Test reading fresh candle data. Will be downloaded from Binance API."""
    df = candle_downloader.fetch_candlestick_data(
        SYMBOL,
        TIME_BUCKET,
        START_AT,
        END_AT,
        force_redownload=True,
    )

    assert len(df) == 49
    assert df.columns.tolist() == ["open", "high", "low", "close", "volume"]
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False


def test_read_cached_candle_data(
    candle_downloader: BinanceDownloader
):
    """Test reading cached candle data. Must be run after test_read_fresh_candle_data."""
    df = candle_downloader.get_data_parquet(SYMBOL, TIME_BUCKET, START_AT, END_AT)

    assert len(df) == 49
    assert df.columns.tolist() == ["open", "high", "low", "close", "volume"]
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False


def test_purge_cached_candle_data(candle_downloader: BinanceDownloader, path):
    """Test purging cached candle data. Must be run after test_read_fresh_candle_data."""
    assert path.exists() == True
    candle_downloader.purge_cached_file(path=path)
    assert path.exists() == False

    candle_downloader.purge_all_cached_data()
    assert len(list(candle_downloader.cache_directory.iterdir())) == 0
