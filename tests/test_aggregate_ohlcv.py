"""Test creating aggregates of volume data across multiple pairs."""
import pandas as pd
import pytest
from pandas.core.groupby import DataFrameGroupBy

from tradingstrategy.utils.aggregate_ohlcv import calculate_volume_weighted_ohlc

# pair_id, timestamp, open, high, low, close, liquidity
example_data = [
    (1, pd.Timestamp("2020-01-01"), 100, 100, 100, 100, 500, 10),
    (1, pd.Timestamp("2020-02-02"), 100, 100, 100, 100, 500, 10),

    (2, pd.Timestamp("2020-01-01"), 110, 110, 110, 110, 250, 20),
    (2, pd.Timestamp("2020-02-02"), 110, 110, 110, 110, 250, 20),

    (3, pd.Timestamp("2020-02-02"), 200, 200, 200, 200, 1000, 30),
]


@pytest.fixture(scope="module")
def pair_timestamp_df() -> DataFrameGroupBy:
    df = pd.DataFrame(example_data, columns=["pair_id", "timestamp", "open", "high", "low", "close", "volume", "liquidity"])
    df = df.set_index("timestamp")
    return df


def test_calculate_volume_weighted_ohlc(pair_timestamp_df: pd.DataFrame):
    aggregate_ohlcvl = calculate_volume_weighted_ohlc(pair_timestamp_df)

    #                   open        high         low       close  volume  liquidity
    # timestamp
    # 2020-01-01  103.333333  103.333333  103.333333  103.333333     750         30
    # 2020-02-02  158.571429  158.571429  158.571429  158.571429    1750         60

    assert aggregate_ohlcvl["open"][pd.Timestamp("2020-01-01")] == pytest.approx(103.333333)
    assert aggregate_ohlcvl["volume"][pd.Timestamp("2020-01-01")] == pytest.approx(750)
    assert aggregate_ohlcvl["liquidity"][pd.Timestamp("2020-01-01")] == pytest.approx(30)
    assert aggregate_ohlcvl["liquidity"][pd.Timestamp("2020-02-02")] == pytest.approx(60)
