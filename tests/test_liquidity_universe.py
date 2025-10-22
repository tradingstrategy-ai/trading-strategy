"""TVL API tests."""

import datetime
import os

import pandas as pd
import pytest

from tradingstrategy.candle import GroupedCandleUniverse
from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.liquidity import GroupedLiquidityUniverse, LiquidityDataUnavailable
from tradingstrategy.pair import DEXPair, PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.transport.cache import OHLCVCandleType
from tradingstrategy.utils.forward_fill import forward_fill
from tradingstrategy.utils.liquidity_filter import build_liquidity_summary
from tradingstrategy.utils.token_filter import filter_pairs_default


CI = os.environ.get("CI") == "true"


pytestmark = pytest.mark.skipif(CI, reason="Too slow on Github")


def test_grouped_liquidity(
    persistent_test_client: Client,
    default_pair_universe,
):
    """Group downloaded liquidity sample data by a trading pair."""

    pair_universe = default_pair_universe

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()

    liquidity_universe = GroupedLiquidityUniverse(raw_liquidity_samples)

    # Do some test calculations for a single pair
    sushi_swap = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "sushi")
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
    assert min_liq > 10

    # Test by sample by timestamp
    amount, delay = liquidity_universe.get_liquidity_with_tolerance(
        sushi_usdt.pair_id,
        pd.Timestamp("2021-12-31"),
        tolerance=pd.Timedelta(days=365),
    )
    assert amount == pytest.approx(2292.4517)
    assert delay == pd.Timedelta('4 days 00:00:00')

    # Test that we get a correct exception by asking non-existing timestamp
    with pytest.raises(LiquidityDataUnavailable):
        liquidity_universe.get_liquidity_with_tolerance(
            sushi_usdt.pair_id,
            pd.Timestamp("1970-01-01"),
            tolerance=pd.Timedelta(days=360),
        )


def test_combined_candles_and_liquidity(persistent_test_client: Client):
    """Candles and liquidity data looks similar regarding the pair count."""

    client = persistent_test_client

    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()
    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()

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

    # TVL data missing for many chains since switching to TVL method 2.0
    assert abs((liq_pair_count - candle_pair_count) / liq_pair_count) < 0.50


def test_liquidity_index_is_datetime(
    persistent_test_client: Client,
    default_pair_universe,
    default_exchange_universe,
):
    """Any liquidity samples use datetime index by default.

    Avoid raw running counter indexes. This makes manipulating data much easier.
    """
    client = persistent_test_client

    pair_universe = default_pair_universe
    exchange_universe = default_exchange_universe

    pair = pair_universe.get_pair_by_human_description([ChainId.ethereum, "uniswap-v2", "WETH", "DAI"])

    exchange = exchange_universe.get_by_chain_and_slug(ChainId.ethereum, "uniswap-v2")
    assert exchange, "Uniswap v2 not found"

    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()
    liquidity_universe = GroupedLiquidityUniverse(raw_liquidity_samples)
    liq1 = liquidity_universe.get_liquidity_samples_by_pair(pair.pair_id)
    assert isinstance(liq1.index, pd.DatetimeIndex)


def test_merge_liquidity_samples(
    persistent_test_client: Client,
    default_exchange_universe,
    default_pairs_df,
    default_pair_universe,
):
    """Merging two liquidity graphs using Pandas should work.


    See also liquidity-analysis.ipynb
    """

    client = persistent_test_client

    exchange_universe = default_exchange_universe

    uniswap_v2 = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "uniswap v2")
    sushi_swap = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "sushi")

    raw_pairs = default_pairs_df

    pair_universe = default_pair_universe

    pair1: DEXPair = pair_universe.get_one_pair_from_pandas_universe(
        sushi_swap.exchange_id,
        "AAVE",
        "WETH")

    # Uniswap has fake listings for AAVE-WETH, and
    # pick_by_highest_vol=True will work around this by
    # using the highest volume pair of the same name.
    # Usually the real pair has the highest volume and
    # scam tokens have ~0 volume.
    pair2: DEXPair = pair_universe.get_one_pair_from_pandas_universe(
        uniswap_v2.exchange_id,
        "AAVE",
        "WETH",
        pick_by_highest_vol=True)

    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()
    liquidity_universe = GroupedLiquidityUniverse(raw_liquidity_samples)
    liq1 = liquidity_universe.get_liquidity_samples_by_pair(pair1.pair_id)
    liq2 = liquidity_universe.get_liquidity_samples_by_pair(pair2.pair_id)

    sushi = liq1[["close"]] / 1_000_000
    uni = liq2[["close"]] / 1_000_000

    # Merge using timestamp index
    df = pd.merge_ordered(sushi, uni, fill_method="ffill")
    df = df.rename(columns={"close_x": "Sushi", "close_y": "Uni"})


def test_empty_liquididty_universe():
    universe = GroupedLiquidityUniverse.create_empty()
    assert universe.get_sample_count() == 0


def test_build_liquidity_summary(
    persistent_test_client: Client,
    default_exchange_universe,
    default_pairs_df,
):
    """See we can put together historical liquidity for backtest filtering.

    - Get liquidity summary for all Uniswap v3 pairs on Etheruem
    """

    client = persistent_test_client

    exchange_universe = default_exchange_universe

    exchange = exchange_universe.get_by_chain_and_slug(ChainId.ethereum, "uniswap-v3")
    pairs_df = default_pairs_df

    pairs_df = filter_pairs_default(
        pairs_df,
        chain_id=ChainId.ethereum,
        exchanges={exchange},
        verbose_print=lambda x, y: x,  # Mute
    )

    liquidity_df = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()
    liquidity_df = liquidity_df.loc[liquidity_df["pair_id"].isin(pairs_df["pair_id"])]  # Filter to our pair set before forward fill
    liquidity_df = liquidity_df.set_index("timestamp").groupby("pair_id")
    ff_liquidity_df = forward_fill(liquidity_df, TimeBucket.d7.to_frequency(), columns=("close",))

    # assert isinstance(liquidity_df.obj.index, type(ff_liquidity_df.obj.index)), f"Index type changes: {type(liquidity_df.index)} -> {type(ff_liquidity_df.index)}"

    # TODO: We lose some pairs because they have no data?
    pairs_original = sorted(pairs_df["pair_id"].unique())
    pairs_after_ff = sorted(ff_liquidity_df.obj["pair_id"].unique())
    # assert pairs_original == pairs_after_ff, "Pair IDs do not match after forward fill"

    historical_max, today = build_liquidity_summary(ff_liquidity_df, pairs_df["pair_id"])
    assert len(historical_max) > 100

    for pair_id, liquidity_usd in historical_max.most_common(10):
        assert liquidity_usd > 0, f"Got zero liquidity for pair {pair_id}"


def test_load_tvl_one_pair(
    persistent_test_client: Client,
    default_exchange_universe,
    default_pair_universe,
):
    """Load TVL data for a single pair."""

    client = persistent_test_client

    pair_universe = default_pair_universe

    pair = pair_universe.get_pair_by_human_description(
        (ChainId.ethereum, "uniswap-v3", "WETH", "USDC", 0.0005)
    )

    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 2, 1)

    liquidity_df = client.fetch_tvl_by_pair_ids(
        [pair.pair_id],
        TimeBucket.d1,
        start_time=start,
        end_time=end,
    )

    assert len(liquidity_df) == 32


def test_load_tvl_one_pair_cache(
    persistent_test_client: Client,
    default_exchange_universe,
    default_pair_universe,
):
    """Load TVL data for a single pair, use cache."""

    client = persistent_test_client

    pair_universe = default_pair_universe

    pair = pair_universe.get_pair_by_human_description(
        (ChainId.ethereum, "uniswap-v3", "WETH", "USDC", 0.0005)
    )

    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 2, 1)

    _ = client.fetch_tvl_by_pair_ids(
        [pair.pair_id],
        TimeBucket.d1,
        start_time=start,
        end_time=end,
    )

    # Should do cached now, but
    # we really do not check
    liquidity_df = client.fetch_tvl_by_pair_ids(
        [pair.pair_id],
        TimeBucket.d1,
        start_time=start,
        end_time=end,
    )

    assert len(liquidity_df) == 32


def test_load_tvl_two_pairs_mixed_exchange(
    persistent_test_client: Client,
    default_exchange_universe,
    default_pair_universe,

):
    """Load TVL data for two pairs using a different DEX rtype."""

    client = persistent_test_client

    pair_universe = default_pair_universe

    pair = pair_universe.get_pair_by_human_description(
        (ChainId.ethereum, "uniswap-v3", "WETH", "USDC", 0.0005)
    )

    pair_2 = pair_universe.get_pair_by_human_description(
        (ChainId.ethereum, "uniswap-v2", "WETH", "USDC")
    )

    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 2, 1)

    liquidity_df = client.fetch_tvl_by_pair_ids(
        [pair.pair_id, pair_2.pair_id],
        TimeBucket.d1,
        start_time=start,
        end_time=end,
    )

    assert len(liquidity_df) == 64


def test_uniswap_v2_weth_quoted_tvl(
    persistent_test_client: Client,
    default_pair_universe,
):
    """See that v2 liquidity can be quried."""

    client = persistent_test_client
    pair_universe = default_pair_universe

    # https://tradingstrategy.ai/trading-view/arbitrum/uniswap-v3/pepe-eth-fee-100-2
    pair = pair_universe.get_pair_by_human_description(
        (ChainId.ethereum, "uniswap-v3", "PEPE", "WETH", 0.01)
    )

    start = datetime.datetime(2024, 8, 1)
    end = datetime.datetime(2024, 9, 1)

    # v2 query works
    liquidity_df = client.fetch_tvl_by_pair_ids(
        [pair.pair_id],
        TimeBucket.d1,
        start_time=start,
        end_time=end,
        query_type=OHLCVCandleType.tvl_v2,
    )
    assert len(liquidity_df) > 0

    # v1 query does not work
    liquidity_df = client.fetch_tvl_by_pair_ids(
        [pair.pair_id],
        TimeBucket.d1,
        start_time=start,
        end_time=end,
        query_type=OHLCVCandleType.tvl_v1,
    )

    assert len(liquidity_df) == 0
