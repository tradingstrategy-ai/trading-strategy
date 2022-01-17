import pandas as pd
import pytest

from tradingstrategy.candle import GroupedCandleUniverse
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.client import Client
from tradingstrategy.chain import ChainId
from tradingstrategy.pair import PairUniverse, PandasPairUniverse


def test_grouped_candles(persistent_test_client: Client):
    """Group downloaded candles by a trading pair."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    raw_pairs = client.fetch_pair_universe().to_pandas()
    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()

    pair_universe = PandasPairUniverse(raw_pairs)
    candle_universe = GroupedCandleUniverse(raw_candles)

    # Do some test calculations for a single pair
    sushi_swap = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "sushiswap")
    sushi_usdt = pair_universe.get_one_pair_from_pandas_universe(sushi_swap.exchange_id, "SUSHI", "USDT")

    sushi_usdt_candles = candle_universe.get_candles_by_pair(sushi_usdt.pair_id)

    # Get max and min weekly candle of SUSHI-USDT on SushiSwap
    high_price = sushi_usdt_candles["high"]
    max_price = high_price.max()

    low_price = sushi_usdt_candles["low"]
    min_price = low_price.min()

    # Do a timezone / summer time sanity check
    ts_column = sushi_usdt_candles["timestamp"]
    ts_list = ts_column.to_list()
    sample_timestamp = ts_list[0]
    assert sample_timestamp.tz is None
    assert sample_timestamp.tzinfo is None
    assert sample_timestamp.hour == 0
    assert sample_timestamp.minute == 0

    # Min and max prices of SUSHI-USDT ever
    assert max_price == pytest.approx(22.4612)
    assert min_price == pytest.approx(0.49680945)


def test_samples_by_timestamp(persistent_test_client: Client):
    """Get all OHLCV candles at a certain timestamp."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    raw_pairs = client.fetch_pair_universe().to_pandas()
    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()

    pair_universe = PandasPairUniverse(raw_pairs)
    candle_universe = GroupedCandleUniverse(raw_candles)

    # The internal weekly start before Dec 2021
    ts = pd.Timestamp("2021-11-29")
    candles = candle_universe.get_all_samples_by_timestamp(ts)
    assert len(candles) > 1000
    assert candles.iloc[0].timestamp == ts
    assert candles.iloc[0].open > 0
    assert candles.iloc[0].close > 0
    assert candles.iloc[0].buys > 0
    assert candles.iloc[0].sells > 0
