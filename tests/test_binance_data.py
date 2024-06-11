import datetime
import tempfile
import requests
import pytest
import pandas as pd
import os
from pathlib import Path
from unittest.mock import patch

from tradingstrategy.binance.downloader import BinanceDownloader, BinanceDataFetchError
from tradingstrategy.binance.utils import (
    generate_pairs_for_binance,
    add_info_columns_to_ohlc,
    generate_lending_reserve_for_binance,
)
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.chain import ChainId


CANDLE_SYMBOL = "ETHUSDC"
LENDING_SYMBOL = "ETH"
LENDING_TIME_BUCKET = TimeBucket.d1
TIME_BUCKET = TimeBucket.d1
START_AT = datetime.datetime(2021, 1, 1)
END_AT = datetime.datetime(2021, 1, 2)


# Because of geoblocks, only run tests with we enable Binance Margin API specifically
pytestmark = pytest.mark.skipif(not os.environ.get("BASE_BINANCE_MARGIN_API_URL"), reason="Set BASE_BINANCE_MARGIN_API_URL to run these tests to a proxy server or https://www.binance.com/bapi/margin")


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
            "pair_id": {
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
    assert df.columns.tolist() == list(correct_df.columns)
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
            "pair_id": ["ETHUSDC", "ETHUSDC", "BTCUSDC", "BTCUSDC"],
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
    assert df.columns.tolist() == list(correct_df.columns)
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
    assert df.columns.tolist() == ["open", "high", "low", "close", "volume", "pair_id"]
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False

    assert df.index[0].to_pydatetime() == START_AT
    assert df.index[-1].to_pydatetime() == END_AT


def test_read_fresh_lending_data(candle_downloader: BinanceDownloader):
    """Test reading fresh lending data. Will be downloaded from Binance API.

    This is to check that the lending data is correct i.e. correct time bucket, no missing values
    """
    correct_df = pd.DataFrame(
        {
            "lending_rates": {
                pd.Timestamp("2021-01-01 00:00:00"): 9.125,
                pd.Timestamp("2021-01-02 00:00:00"): 9.125,
            },
            "pair_id": {
                pd.Timestamp("2021-01-01 00:00:00"): "ETH",
                pd.Timestamp("2021-01-02 00:00:00"): "ETH",
            },
        }
    )

    if os.environ.get("GITHUB_ACTIONS", None) == "true":
        with patch(
            "tradingstrategy.binance.downloader.BinanceDownloader.fetch_lending_rates"
        ) as mock_fetch_candlestick_data:
            mock_fetch_candlestick_data.return_value = correct_df

            df = candle_downloader.fetch_lending_rates(
                LENDING_SYMBOL,
                LENDING_TIME_BUCKET,
                START_AT,
                END_AT,
                force_download=True,
            )

            path = candle_downloader.get_parquet_path(
                LENDING_SYMBOL, LENDING_TIME_BUCKET, START_AT, END_AT, is_lending=True
            )
            df.to_parquet(path)
    else:
        df = candle_downloader.fetch_lending_rates(
            LENDING_SYMBOL,
            LENDING_TIME_BUCKET,
            START_AT,
            END_AT,
            force_download=True,
        )

    assert df.equals(correct_df)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False

    assert df.index[0].to_pydatetime() == START_AT
    assert df.index[-1].to_pydatetime() == END_AT


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
    assert df.index[0].to_pydatetime() == START_AT
    assert df.index[-1].to_pydatetime() == END_AT


@pytest.mark.skipif(os.environ.get("GITHUB_ACTIONS", None) == "true", reason="Only run locally")
def test_url_reponse():
    """Check that binance data doesn't change or return errors."""
    base_url = os.environ.get("BASE_BINANCE_MARGIN_API_URL")
    url = f'{base_url}/v1/public/margin/vip/spec/history-interest-rate?asset=BTC&vipLevel=0&size=90&startTime=1640988000000&endTime=1643666400000'

    response = requests.get(url)
    json_data = response.json()

    assert json_data['success'] == True
    assert json_data['code'] == '000000'
    assert json_data['message'] == None
    assert json_data['messageDetail'] == None
    assert json_data['data'] == [
    {'asset': 'BTC', 'timestamp': '1643587200000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1643500800000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1643414400000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1643328000000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1643241600000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1643155200000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1643068800000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1642982400000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1642896000000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1642809600000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1642723200000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1642636800000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1642550400000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1642464000000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1642377600000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1642291200000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1642204800000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1642118400000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1642032000000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1641945600000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1641859200000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1641772800000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1641686400000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1641600000000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1641513600000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1641427200000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1641340800000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1641254400000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1641168000000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1641081600000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1640995200000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'},
    {'asset': 'BTC', 'timestamp': '1640908800000', 'dailyInterestRate': '0.00020000', 'vipLevel': '0'}
]

    

@pytest.mark.skipif(os.environ.get("GITHUB_ACTIONS", None) == "true", reason="Only run locally")
def test_read_fresh_lending_data_local_only(request, candle_downloader: BinanceDownloader):
    df = candle_downloader.fetch_lending_rates(
        {'USDT', 'BTC'},
        TimeBucket.h1,
        datetime.datetime(2024, 1, 1),
        datetime.datetime(2024, 5, 2),
        force_download=True,
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5858
    assert df.isna().sum().sum() == 0
    assert df.isna().values.any() == False


    assert df.index[0] == pd.Timestamp("2024-01-01 00:00:00")
    assert df.index[-1] == pd.Timestamp("2024-05-02 00:00:00")



def test_purge_cache():
    """Test purging cached candle data. Must be run after test_read_fresh_candle_data and test_read_cached_candle_data.

    - Checks that deleting cached data works correctly.
    - Must run in its isolated directory so that it does not delete data from other tests
    """

    with tempfile.TemporaryDirectory() as tmpdirname:
        candle_downloader = BinanceDownloader(Path(tmpdirname))

        candle_path = candle_downloader.get_parquet_path(
            CANDLE_SYMBOL, TIME_BUCKET, START_AT, END_AT
        )
        candle_path.open("wt").write("foo")
        assert candle_path.exists() == True, f"Did not exist {candle_path}"
        candle_downloader.purge_cached_file(path=candle_path)
        assert candle_path.exists() == False

        lending_path = candle_downloader.get_parquet_path(
            LENDING_SYMBOL, LENDING_TIME_BUCKET, START_AT, END_AT, is_lending=True
        )
        lending_path.open("wt").write("foo")
        assert lending_path.exists() == True, f"Did not exist: {lending_path}"
        candle_downloader.purge_cached_file(path=lending_path)
        assert lending_path.exists() == False

        candle_downloader.purge_all_cached_data()
        assert len(list(candle_downloader.cache_directory.iterdir())) == 0


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


def test_starting_date_unknown(candle_downloader: BinanceDownloader):
    """Test purging cached candle data. Must be run after test_read_fresh_candle_data and test_read_cached_candle_data.

    Checks that deleting cached data works correctly.
    """

    # Unknown asset
    with pytest.raises(BinanceDataFetchError):
        candle_downloader.fetch_approx_asset_trading_start_date("FOOBAR")


def test_fetch_assets(candle_downloader: BinanceDownloader):
    """Get available tradeable assets on Binance."""
    assets = list(candle_downloader.fetch_assets())
    assert "BTCUSDT" in assets
    assert "ETHUSDT" in assets

    # 484 tickers at the end of 2023
    assert len(assets) >= 484

    spot_symbols = list(candle_downloader.fetch_assets("SPOT"))
    assert len(spot_symbols) >= 2331

    lending_symbols = list(candle_downloader.fetch_all_lending_symbols())
    assert len(lending_symbols) >= 312


def test_add_info_columns():
    """Check that add_info_columns_to_ohlc works correctly."""
    symbols = [CANDLE_SYMBOL]

    pairs = generate_pairs_for_binance(symbols)

    df = pd.DataFrame(
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
            "pair_id": {
                pd.Timestamp("2021-01-01"): "ETHUSDC",
                pd.Timestamp("2021-01-02"): "ETHUSDC",
            },
        }
    )

    assert len(df.columns) == 6

    candle_df = add_info_columns_to_ohlc(
        df, {symbol: pair for symbol, pair in zip(symbols, pairs)}
    )

    assert len(df.columns) == 20
    assert candle_df.isna().sum().sum() == 0


def test_generate_lending_reserve():
    """Check that generate_lending_reserve_for_binance works correctly."""
    reserve = generate_lending_reserve_for_binance(
        "ETH", "0x4b2d72c1cb89c0b2b320c43bb67ff79f562f5ff4", 1
    )
    assert reserve.chain_id == ChainId.centralised_exchange


def test_fetch_binance_price_data_multipair():
    """Check that pair data for multipair looks correct.

    - Download both BTC and ETH data and check it looks sane
    """

    downloader = BinanceDownloader()
    df = downloader.fetch_candlestick_data(
        ["BTCUSDT", "ETHUSDT"],
        TimeBucket.d1,
        datetime.datetime(2020, 1, 1),
        datetime.datetime(2021, 1, 1),
    )

    # Recorded 2/2024
    assert df.iloc[0].to_dict() == {'open': 7195.24, 'high': 7255.0, 'low': 7175.15, 'close': 7200.85, 'volume': 16792.388165, 'pair_id': 'BTCUSDT'}
    assert df.iloc[-1].to_dict() == {'open': 736.42, 'high': 749.0, 'low': 714.29, 'close': 728.91, 'volume': 675114.09329, 'pair_id': 'ETHUSDT'}

