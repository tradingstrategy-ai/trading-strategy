"""Slippage calculation test suite."""
import pytest
import pandas as pd

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.liquidity import GroupedLiquidityUniverse
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.priceimpact import estimate_xyk_price_impact, HistoricalXYPriceImpactCalculator, \
    LiquidityDataMissing
from tradingstrategy.timebucket import TimeBucket


def test_calculate_slippage_simple():
    """Simple slippage calculation example.

    https://dailydefi.org/articles/price-impact-and-how-to-calculate/

    Pool info::

        USDC = 2,000,000
        ETH = 1,000
        Constant Product = 2,000,000,000
        Market Price = 2,000

    We sell 100,000 USDC worth of ETH.
    """

    # 30 bps
    uniswap_v2_lp_fee = 0.0030
    impact = estimate_xyk_price_impact(2_000_000, 100_000, uniswap_v2_lp_fee)
    assert impact.delivered == pytest.approx(94965.94751631189)
    assert impact.lp_fees_paid == pytest.approx(300.0)
    assert impact.slippage_amount == pytest.approx(5034.052483688109)


def test_calculate_slippage_from_dataset(persistent_test_client: Client):
    """Calculate the slippage from a downloaded dataset.
    """

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    raw_pairs = client.fetch_pair_universe().to_pandas()
    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()

    pair_universe = PandasPairUniverse(raw_pairs)
    liquidity_universe = GroupedLiquidityUniverse(raw_liquidity_samples)

    # Do some test calculations for a single pair
    sushi_swap = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "sushiswap")
    sushi_eth = pair_universe.get_one_pair_from_pandas_universe(sushi_swap.exchange_id, "SUSHI", "WETH")

    price_impact_calculator = HistoricalXYPriceImpactCalculator(liquidity_universe)

    # SUSHI-WETH started trading around 2020-09-01
    trading_date = pd.Timestamp("2021-06-01")
    trade_size = 6000.0  # USD

    impact = price_impact_calculator.calculate_price_impact(trading_date, sushi_eth.pair_id, trade_size)
    print(impact)
    import ipdb ; ipdb.set_trace()


def test_unknown_pair(persistent_test_client: Client):
    """We get an exception if we ask price impact for an unknown pair."""

    client = persistent_test_client

    raw_pairs = client.fetch_pair_universe().to_pandas()
    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()

    liquidity_universe = GroupedLiquidityUniverse(raw_liquidity_samples)

    price_impact_calculator = HistoricalXYPriceImpactCalculator(liquidity_universe)

    # SUSHI-WETH started trading around 2020-09-01
    trading_date = pd.Timestamp("2021-06-01")
    trade_size = 6000.0  # USD

    with pytest.raises(LiquidityDataMissing):
        price_impact_calculator.calculate_price_impact(trading_date, 0, trade_size)


def test_calculate_slippage_sample_too_far_in_past(persistent_test_client: Client):
    """We get an exception if we ask for a sample for a date where date is not available."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    raw_pairs = client.fetch_pair_universe().to_pandas()
    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()

    pair_universe = PandasPairUniverse(raw_pairs)
    liquidity_universe = GroupedLiquidityUniverse(raw_liquidity_samples)

    # Do some test calculations for a single pair
    sushi_swap = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "sushiswap")
    sushi_eth = pair_universe.get_one_pair_from_pandas_universe(sushi_swap.exchange_id, "SUSHI", "WETH")

    price_impact_calculator = HistoricalXYPriceImpactCalculator(liquidity_universe)

    # SUSHI-WETH started trading around 2020-09-01
    trading_date = pd.Timestamp("2021-06-01")
    trade_size = 6000.0  # USD

    impact = price_impact_calculator.calculate_price_impact(trading_date, sushi_eth.pair_id, trade_size)
