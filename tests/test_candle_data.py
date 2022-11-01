"""Synthetic candle data tests."""

import pandas as pd
import pytest

from tradingstrategy.candle import Candle, GroupedCandleUniverse


@pytest.fixture()
def synthetic_candles() -> pd.DataFrame:
    """Some hand-written test data.

    Contains candle data for one trading pair (pair_id=1)
    """

    data = [
        Candle.generate_synthetic_sample(1, pd.Timestamp("2020-01-01"), 100.10),
        Candle.generate_synthetic_sample(1, pd.Timestamp("2020-02-01"), 98.20),
    ]

    df = pd.DataFrame(data, columns=Candle.DATAFRAME_FIELDS)
    return df


def test_generate_candle_data(synthetic_candles):
    """Test creation of candles."""

    universe = GroupedCandleUniverse(synthetic_candles)

    assert universe.get_pair_count() == 1
    assert universe.get_candle_count() == 2
    df = universe.get_candles_by_pair(pair_id=1)
    assert df.loc[pd.Timestamp("2020-01-01")]["open"] == 100.10
    assert df.loc[pd.Timestamp("2020-02-01")]["close"] == 98.20



