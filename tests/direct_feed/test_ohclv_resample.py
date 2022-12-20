"""OHLCV generation tests."""
import random
from dataclasses import asdict
from decimal import Decimal

import pandas as pd
import pytest

from tradingstrategy.direct_feed.conversion import CurrencyConversion, convert_to_float
from tradingstrategy.direct_feed.ohlcv_aggregate import resample_trades_into_ohlcv, get_feed_for_pair, truncate_ohlcv
from tradingstrategy.direct_feed.timeframe import Timeframe
from tradingstrategy.direct_feed.trade_feed import Trade
from tradingstrategy.direct_feed.warn import disable_pandas_warnings


@pytest.fixture
def test_df():
    """Test trade feed with 1 trade"""

    trades = [
        Trade(
            "AAVE-ETH",
            1,
            hex(random.randint(2**31, 2**32)),
            pd.Timestamp("2020-01-01 01:00"),
            "0xff",
            1,
            Decimal(0.05),  # 0.05 ETH/AAVE
            Decimal(0.8),  # 0.8 worth of ETH
            Decimal(1600),  # 1600 USD/ETH
        ),

        Trade(
            "AAVE-ETH",
            2,
            hex(random.randint(2**31, 2**32)),
            pd.Timestamp("2020-01-02 02:00"),
            "0xff",
            1,
            Decimal(0.06),  # 0.05 ETH/AAVE
            Decimal(0.7),  # 0.8 worth of ETH
            Decimal(1600),  # 1600 USD/ETH
        ),

        Trade(
            "ETH-USDC",
            2,
            hex(random.randint(2**31, 2**32)),
            pd.Timestamp("2020-01-02 02:00"),
            "0xff",
            2,
            Decimal(1600),  # 1600 USD/CETH
            Decimal(5000),  # 5000 USD worth of ETH buy
            Decimal(1),  # 1 USDC/USD
        ),

        Trade(
            "ETH-USDC",
            7,
            hex(random.randint(2**31, 2**32)),
            pd.Timestamp("2020-01-05 15:00"),
            "0xff",
            1,
            Decimal(1620),  # 1600 USD/CETH
            Decimal(-2000),  # 5000 USD worth of ETH buy
            Decimal(1),  # 1 USDC/USD
        ),

        Trade(
            "ETH-USDC",
            8,
            hex(random.randint(2**31, 2**32)),
            pd.Timestamp("2020-01-05 15:00"),
            "0xff",
            1,
            Decimal(1400),  # 1600 USD/CETH
            Decimal(250),  # 5000 USD worth of ETH buy
            Decimal(1),  # 1 USDC/USD
        )
    ]

    df = pd.DataFrame([asdict(t) for t in trades],
                      columns=list(Trade.get_dataframe_columns().keys()))
    df.set_index("block_number", inplace=True, drop=False)
    return df


def test_ohlcv_resample(test_df):
    """Generate OHLCV candles."""
    df = convert_to_float(test_df, CurrencyConversion.us_dollar)
    timeframe = Timeframe("1D")
    ohlcv = resample_trades_into_ohlcv(df, timeframe)

    #                        high     low   close  exchange_rate  start_block  end_block
    # pair     timestamp
    # AAVE-ETH 2020-01-01    80.0    80.0    80.0         1600.0            1          1
    #          2020-01-02    96.0    96.0    96.0         1600.0            2          2
    # ETH-USDC 2020-01-02  1600.0  1600.0  1600.0            1.0            2          2

    aave = get_feed_for_pair(ohlcv, "AAVE-ETH")
    assert len(aave) == 2 # We have 2 days worth of data

    record = aave.iloc[0]
    assert record["high"] == 80
    assert record["low"] == 80
    assert record["exchange_rate"] == 1600
    assert record["start_block"] == 1
    assert record["end_block"] == 1
    assert record["volume"] == 1280
    assert record["buys"] == 1
    assert record["sells"] == 0

    eth = get_feed_for_pair(ohlcv, "ETH-USDC")
    assert len(eth) == 2 # We have 2 days worth of data
    record = eth.iloc[-1]
    assert record["high"] == 1620
    assert record["low"] == 1400
    assert record["exchange_rate"] == 1
    assert record["start_block"] == 7
    assert record["end_block"] == 8
    assert record["volume"] == 2250
    assert record["buys"] == 1
    assert record["sells"] == 1


def test_ohlcv_truncate(test_df):
    """OHLCV candles can be truncated and reinserted."""
    df = convert_to_float(test_df, CurrencyConversion.us_dollar)
    timeframe = Timeframe("1D")
    ohlcv = resample_trades_into_ohlcv(df, timeframe)
    ohlcv = truncate_ohlcv(ohlcv, pd.Timestamp("2020-01-04"))
    eth = get_feed_for_pair(ohlcv, "ETH-USDC")
    assert len(eth) == 1
    aave = get_feed_for_pair(ohlcv, "AAVE-ETH")
    assert len(aave) == 2 # We have 2 days worth of data
