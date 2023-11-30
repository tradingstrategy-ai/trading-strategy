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

    correct_df = pd.DataFrame(
        {
            "open": {
                pd.Timestamp("2021-01-01"): 736.9,
                pd.Timestamp("2021-01-02"): 731.19,
            },
            "high": {
                pd.Timestamp("2021-01-01"): 750.39,
                pd.Timestamp("2021-01-02"): 788.89,
            },
            "low": {
                pd.Timestamp("2021-01-01"): 714.86,
                pd.Timestamp("2021-01-02"): 716.71,
            },
            "close": {
                pd.Timestamp("2021-01-01"): 730.79,
                pd.Timestamp("2021-01-02"): 774.73,
            },
            "volume": {
                pd.Timestamp("2021-01-01"): 15151.39095,
                pd.Timestamp("2021-01-02"): 26362.64832,
            },
            "symbol": {
                pd.Timestamp("2021-01-01"): "ETHUSDC",
                pd.Timestamp("2021-01-02"): "ETHUSDC",
            },
        }
    )

    if os.environ.get("GITHUB_ACTIONS", None) == "true":
        with patch(
            "tradingstrategy.binance.downloader.BinanceDownloader.fetch_candlestick_data"
        ) as mock_fetch_candlestick_data:
            mock_fetch_candlestick_data.return_value = correct_df

            df = candle_downloader.fetch_candlestick_data(
                CANDLE_SYMBOL,
                TIME_BUCKET,
                START_AT,
                END_AT,
                force_download=True,
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
            force_download=True,
        )

    assert df.equals(correct_df)
    assert len(df) == 2
    assert df.columns.tolist() == ["open", "high", "low", "close", "volume", "symbol"]
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False


def test_read_fresh_candle_data_multipair(candle_downloader: BinanceDownloader):
    """Test reading fresh candle data.

    Will be mock data if run on Github, otherwise will be downloadeded from Binance API if local.

    This is to check that the candle data is correct i.e. correct time bucket, no missing values, correct columns etc
    """

    correct_df = pd.DataFrame(
        {
            "open": [736.9, 731.19, 28964.54, 29393.99],
            "high": [750.39, 788.89, 29680.0, 33500.0],
            "low": [714.86, 716.71, 28608.73, 29027.03],
            "close": [730.79, 774.73, 29407.93, 32215.18],
            "volume": [15151.39095, 26362.64832, 1736.62048, 4227.234681],
            "symbol": ["ETHUSDC", "ETHUSDC", "BTCUSDC", "BTCUSDC"],
        },
        index=[
            pd.Timestamp("2021-01-01 00:00:00"),
            pd.Timestamp("2021-01-02 00:00:00"),
            pd.Timestamp("2021-01-01 00:00:00"),
            pd.Timestamp("2021-01-02 00:00:00"),
        ],
    )

    if os.environ.get("GITHUB_ACTIONS", None) == "true":
        with patch(
            "tradingstrategy.binance.downloader.BinanceDownloader.fetch_candlestick_data"
        ) as mock_fetch_candlestick_data:
            mock_fetch_candlestick_data.return_value = correct_df

            df = candle_downloader.fetch_candlestick_data(
                CANDLE_SYMBOL,
                TIME_BUCKET,
                START_AT,
                END_AT,
                force_download=True,
            )

    else:
        df = candle_downloader.fetch_candlestick_data(
            [CANDLE_SYMBOL, "BTCUSDC"],
            TIME_BUCKET,
            START_AT,
            END_AT,
            force_download=True,
        )

    assert df.equals(correct_df)
    assert len(df) == 4
    assert df.columns.tolist() == ["open", "high", "low", "close", "volume", "symbol"]
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
    assert df.columns.tolist() == ["open", "high", "low", "close", "volume", "symbol"]
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False


def test_read_fresh_lending_data(candle_downloader: BinanceDownloader):
    """Test reading fresh lending data. Will be downloaded from Binance API.

    This is to check that the lending data is correct i.e. correct time bucket, no missing values
    """
    df = candle_downloader.fetch_lending_rates(
        LENDING_SYMBOL,
        LENDING_TIME_BUCKET,
        START_AT,
        END_AT,
        force_download=True,
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False


def test_read_cached_lending_data(candle_downloader: BinanceDownloader):
    """Test reading cached candle data. Must be run after test_read_fresh_lending_data.

    Checks that the cache is working correctly
    """
    df = candle_downloader.get_data_parquet(
        LENDING_SYMBOL, LENDING_TIME_BUCKET, START_AT, END_AT, is_lending=True
    )

    assert len(df) == 2
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False


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


@pytest.mark.skipif(
    os.environ.get("GITHUB_ACTIONS", None) == "true",
    reason="Github US servers are blocked by Binance",
)
def test_starting_date(candle_downloader: BinanceDownloader):
    """Test purging cached candle data. Must be run after test_read_fresh_candle_data and test_read_cached_candle_data.

    Checks that deleting cached data works correctly.
    """

    # Longest living asset
    btc_starting_date = candle_downloader.fetch_approx_asset_trading_start_date(
        "BTCUSDT"
    )
    assert btc_starting_date == datetime.datetime(2017, 8, 1, 0, 0)

    curve_starting_date = candle_downloader.fetch_approx_asset_trading_start_date(
        "CRVUSDT"
    )
    assert curve_starting_date == datetime.datetime(2020, 8, 1, 0, 0)


@pytest.mark.skipif(
    os.environ.get("GITHUB_ACTIONS", None) == "true",
    reason="Github US servers are blocked by Binance",
)
def test_starting_date_unknown(candle_downloader: BinanceDownloader):
    """Test purging cached candle data. Must be run after test_read_fresh_candle_data and test_read_cached_candle_data.

    Checks that deleting cached data works correctly.
    """

    # Unknown asset
    with pytest.raises(BinanceDataFetchError):
        candle_downloader.fetch_approx_asset_trading_start_date("FOOBAR")


@pytest.mark.skipif(
    os.environ.get("GITHUB_ACTIONS", None) == "true",
    reason="Github US servers are blocked by Binance",
)
def test_fetch_assets(candle_downloader: BinanceDownloader):
    """Get available tradeable assets on Binance."""
    assets = list(candle_downloader.fetch_assets())
    assert "BTCUSDT" in assets
    assert "ETHUSDT" in assets

    # 484 tickers at the end of 2023
    assert len(assets) >= 484

    spot_symbols = list(candle_downloader.fetch_assets('SPOT'))
    assert len(spot_symbols) >= 2331

    lending_symbols = list(candle_downloader.fetch_all_lending_symbols())
    assert len(lending_symbols) >= 312
