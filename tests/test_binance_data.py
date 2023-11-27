import datetime
import pytest
import pandas as pd
import os
from pathlib import Path
from unittest.mock import patch, Mock

from tradingstrategy.binance.downloader import BinanceDownloader, BinanceDataFetchError
from tradingstrategy.timebucket import TimeBucket


CANDLE_SYMBOL = "ETHUSDC"
LENDING_SYMBOL = "ETH"
LENDING_TIME_BUCKET = TimeBucket.d1
TIME_BUCKET = TimeBucket.d1
START_AT = datetime.datetime(2021, 1, 1)
END_AT = datetime.datetime(2021, 1, 2)


@pytest.fixture(scope="module")
def candle_downloader():
    return BinanceDownloader()


def test_read_fresh_candle_data(candle_downloader: BinanceDownloader):
    """Test reading fresh candle data. 
    
    Will be mock data if run on Github, otherwise will be downloadeded from Binance API if local.
    
    This is to check that the candle data is correct i.e. correct time bucket, no missing values, correct columns etc
    """

    if os.environ.get("GITHUB_ACTIONS", None) == "true":
        with patch(
            "tradingstrategy.binance_data.BinanceDownloader.fetch_candlestick_data"
        ) as mock_fetch_candlestick_data:
            mock_fetch_candlestick_data.return_value = pd.DataFrame(
                {
                    "open": {
                        pd.Timestamp("2021-01-01 02:00:00"): 736.9,
                        pd.Timestamp("2021-01-02 02:00:00"): 731.19,
                    },
                    "high": {
                        pd.Timestamp("2021-01-01 02:00:00"): 750.39,
                        pd.Timestamp("2021-01-02 02:00:00"): 788.89,
                    },
                    "low": {
                        pd.Timestamp("2021-01-01 02:00:00"): 714.86,
                        pd.Timestamp("2021-01-02 02:00:00"): 716.71,
                    },
                    "close": {
                        pd.Timestamp("2021-01-01 02:00:00"): 730.79,
                        pd.Timestamp("2021-01-02 02:00:00"): 774.73,
                    },
                    "volume": {
                        pd.Timestamp("2021-01-01 02:00:00"): 15151.39095,
                        pd.Timestamp("2021-01-02 02:00:00"): 26362.64832,
                    },
                }
            )

            df = candle_downloader.fetch_candlestick_data(
                CANDLE_SYMBOL,
                TIME_BUCKET,
                START_AT,
                END_AT,
                force_redownload=True,
            )

            path = candle_downloader.get_parquet_path(
                CANDLE_SYMBOL, TIME_BUCKET, START_AT, END_AT
            )
            df.to_parquet(path)
    else:
        df = candle_downloader.fetch_candlestick_data(
            CANDLE_SYMBOL,
            TIME_BUCKET,
            START_AT,
            END_AT,
            force_redownload=True,
        )

    assert len(df) == 2
    assert df.columns.tolist() == ["open", "high", "low", "close", "volume"]
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False


def test_read_cached_candle_data(candle_downloader: BinanceDownloader):
    """Test reading cached candle data. Must be run after test_read_fresh_candle_data.
    
    Checks that the caching functionality works correctly.
    """
    df = candle_downloader.get_data_parquet(
        CANDLE_SYMBOL, TIME_BUCKET, START_AT, END_AT
    )

    assert len(df) == 2
    assert df.columns.tolist() == ["open", "high", "low", "close", "volume"]
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False


def test_read_fresh_lending_data(candle_downloader: BinanceDownloader):
    """Test reading fresh lending data. Will be downloaded from Binance API.
    
    This is to check that the lending data is correct i.e. correct time bucket, no missing values
    """
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
    """Test reading cached candle data. Must be run after test_read_fresh_lending_data.
    
    Checks that the cache is working correctly
    """
    series = candle_downloader.get_data_parquet(
        LENDING_SYMBOL, LENDING_TIME_BUCKET, START_AT, END_AT, is_lending=True
    )

    assert len(series) == 2
    assert series.isna().sum().sum() == 0
    assert series.isna().values.any() == False


def test_purge_cache(candle_downloader: BinanceDownloader):
    """Test purging cached candle data. Must be run after test_read_fresh_candle_data and test_read_cached_candle_data.
    
    Checks that deleting cached data works correctly.
    """
    candle_path = candle_downloader.get_parquet_path(
        CANDLE_SYMBOL, TIME_BUCKET, START_AT, END_AT
    )
    assert candle_path.exists() == True
    candle_downloader.purge_cached_file(path=candle_path)
    assert candle_path.exists() == False

    lending_path = candle_downloader.get_parquet_path(
        LENDING_SYMBOL, LENDING_TIME_BUCKET, START_AT, END_AT, is_lending=True
    )
    assert lending_path.exists() == True
    candle_downloader.purge_cached_file(path=lending_path)
    assert lending_path.exists() == False

    candle_downloader.purge_all_cached_data()
    assert len(list(candle_downloader.cache_directory.iterdir())) == 0


@pytest.mark.skipif(os.environ.get("GITHUB_ACTIONS", None) == "true", reason="Github US servers are blocked by Binance")
def test_starting_date(candle_downloader: BinanceDownloader):
    """Test purging cached candle data. Must be run after test_read_fresh_candle_data and test_read_cached_candle_data.

    Checks that deleting cached data works correctly.
    """

    # Longest living asset
    btc_starting_date = candle_downloader.fetch_approx_asset_trading_start_date("BTCUSDT")
    assert btc_starting_date == datetime.datetime(2017, 8, 1, 0, 0)

    curve_starting_date = candle_downloader.fetch_approx_asset_trading_start_date("CRVUSDT")
    assert curve_starting_date == datetime.datetime(2020, 8, 1, 0, 0)


@pytest.mark.skipif(os.environ.get("GITHUB_ACTIONS", None) == "true", reason="Github US servers are blocked by Binance")
def test_starting_date_unknown(candle_downloader: BinanceDownloader):
    """Test purging cached candle data. Must be run after test_read_fresh_candle_data and test_read_cached_candle_data.

    Checks that deleting cached data works correctly.
    """

    # Unknown asset
    with pytest.raises(BinanceDataFetchError):
        candle_downloader.fetch_approx_asset_trading_start_date("FOOBAR")


