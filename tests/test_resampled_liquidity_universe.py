"""Optimised liquidity universe tests."""
import pandas as pd
import pytest
from tradingstrategy.candle import GroupedCandleUniverse
from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.liquidity import GroupedLiquidityUniverse, LiquidityDataUnavailable, ResampledLiquidityUniverse
from tradingstrategy.pair import DEXPair, LegacyPairUniverse, PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket


def test_resampled_liquidity_universe(persistent_test_client: Client):
    """Group downloaded liquidity sample data by a trading pair."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    raw_pairs = client.fetch_pair_universe().to_pandas()
    # Do some test calculations for a single pair
    sushi_swap = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "sushi")
    pair_universe = PandasPairUniverse(raw_pairs, build_index=False)
    sushi_usdt = pair_universe.get_one_pair_from_pandas_universe(sushi_swap.exchange_id, "SUSHI", "USDT")

    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()

    # Remove pairs to speed up the test
    filtered_liq_samples = raw_liquidity_samples.loc[raw_liquidity_samples["pair_id"] == sushi_usdt.pair_id]

    resampled_liquidity_universe = ResampledLiquidityUniverse(
        filtered_liq_samples,
        resample_period="30D",  # Month start frequency https://stackoverflow.com/questions/35339139/what-values-are-valid-in-pandas-freq-tags
    )

    #                value
    # timestamp
    # 2020-08-31   1135.985474
    samples = resampled_liquidity_universe.get_samples_by_pair(sushi_usdt.pair_id)
    assert samples.iloc[0]["value"] == pytest.approx(1135.985474)

    assert resampled_liquidity_universe.get_liquidity_fast(sushi_usdt.pair_id, pd.Timestamp("2020-09-02")) == pytest.approx(1135.985474)
