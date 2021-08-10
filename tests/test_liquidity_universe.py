import pytest

from capitalgram.candle import GroupedCandleUniverse
from capitalgram.liquidity import GroupedLiquidityUniverse
from capitalgram.timebucket import TimeBucket
from capitalgram.client import Capitalgram
from capitalgram.chain import ChainId
from capitalgram.pair import PandasPairUniverse


def test_grouped_liquidity(persistent_test_client: Capitalgram):
    """Group downloaded liquidity sample data by a trading pair."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    raw_pairs = client.fetch_pair_universe().to_pandas()
    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()

    pair_universe = PandasPairUniverse(raw_pairs)
    liquidity_universe = GroupedLiquidityUniverse(raw_liquidity_samples)

    # Do some test calculations for a single pair
    sushi_swap = exchange_universe.get_by_name_and_chain(ChainId.ethereum, "sushiswap")
    sushi_usdt = pair_universe.get_one_pair_from_pandas_universe(sushi_swap.exchange_id, "SUSHI", "USDT")

    sushi_usdt_liquidity_samples = liquidity_universe.get_samples_by_pair(sushi_usdt.pair_id)

    # Get max and min weekly candle of SUSHI-USDT on SushiSwap
    high_liq = sushi_usdt_liquidity_samples["high"]
    max_liq = high_liq.max()

    low_liq = sushi_usdt_liquidity_samples["low"]
    min_liq = low_liq.min()

    # Do a timezone / summer time sanity check
    ts_column = sushi_usdt_liquidity_samples["timestamp"]
    ts_list = ts_column.to_list()
    sample_timestamp = ts_list[0]
    assert sample_timestamp.tz is None
    assert sample_timestamp.tzinfo is None
    assert sample_timestamp.hour == 0
    assert sample_timestamp.minute == 0

    # Min and max liquidity of SUSHI-USDT pool ever
    assert max_liq == pytest.approx(31747.889)
    assert min_liq == pytest.approx(550)


def test_combined_candles_and_liquidity(persistent_test_client: Capitalgram):
    """Candles and liquidity data looks similar regarding the pair count."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    raw_pairs = client.fetch_pair_universe().to_pandas()
    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()
    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()

    pair_universe = PandasPairUniverse(raw_pairs)
    liquidity_universe = GroupedLiquidityUniverse(raw_liquidity_samples)
    candle_universe = GroupedCandleUniverse(raw_candles)

    # Check pair information looks more or less correct
    liq_pair_count = liquidity_universe.get_pair_count()
    candle_pair_count = candle_universe.get_pair_count()

    # pair_universe_count = pair_universe.get_count()

    #liq_pairs = list(liquidity_universe.get_pair_ids())
    #candle_pairs = list(candle_universe.get_pair_ids())

    #total_missing = 0
    #for p in liq_pairs:
    #    if p not in candle_pairs:
    #        info = pair_universe.get_pair_by_id(p)
    #        print(f"Missing in candles: {info}")
    #        total_missing += 1
    #

    # Should be 95% same, some minor differences because of token
    # where liquidity was added but which never traded
    print(liq_pair_count, candle_pair_count)
    assert abs((liq_pair_count - candle_pair_count) / liq_pair_count) < 0.05