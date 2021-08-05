import pytest

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

    sushi_usdt_liquidity_samples = liquidity_universe.get_liquidity_by_pair(sushi_usdt.pair_id)

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
    # 403M liquidity
    assert max_liq == pytest.approx(403_601_060)
    assert min_liq == pytest.approx(49.276726)
