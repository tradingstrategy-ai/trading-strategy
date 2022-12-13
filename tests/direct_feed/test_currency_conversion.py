from dataclasses import asdict
from decimal import Decimal

import pandas as pd
import pytest

from tradingstrategy.direct_feed.conversion import CurrencyConversion, convert_to_float
from tradingstrategy.direct_feed.trade_feed import Trade


@pytest.fixture
def test_df():
    """Test trade feed with 1 trade"""
    t = Trade(
            "AAVE-ETH",
            1,
            "0x1",
            pd.Timestamp("2020-01-01"),
            "0xff",
            1,
            Decimal(0.05),  # 0.05 ETH/AAVE
            Decimal(0.8),  # 0.8 worth of ETH
            Decimal(1600),  # 1600 USD/ETH
        )

    df = pd.DataFrame([asdict(t)], columns=list(Trade.get_dataframe_columns().keys()))
    df.set_index("block_number", inplace=True, drop=False)
    assert len(df) == 1
    return df



def test_convert_prices(test_df):
    """We can convert prices to USD."""
    df2 = convert_to_float(test_df, CurrencyConversion.us_dollar)
    record = df2.iloc[0]
    assert record["price"] == 0.05 * 1600
    assert record["amount"] == 0.8 * 1600


def test_keep_crypto_prices(test_df):
    """We can keep native crypto prices."""
    df2 = convert_to_float(test_df, CurrencyConversion.crypto_quote_token)
    record = df2.iloc[0]
    assert record["price"] == 0.05
    assert record["amount"] == 0.8
    assert df2.index.values[0] == 1