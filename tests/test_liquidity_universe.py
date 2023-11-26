import pandas as pd
import pytest
from tradingstrategy.candle import GroupedCandleUniverse
from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.liquidity import GroupedLiquidityUniverse, LiquidityDataUnavailable
from tradingstrategy.pair import DEXPair, LegacyPairUniverse, PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket


def test_grouped_liquidity(persistent_test_client: Client):
    """Group downloaded liquidity sample data by a trading pair."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    raw_pairs = client.fetch_pair_universe().to_pandas()
    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()

    pair_universe = PandasPairUniverse(raw_pairs, build_index=False)
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

    # Should be 95% same, some minor differences because of token
    # where liquidity was added but which never traded
    # print(liq_pair_count, candle_pair_count)
    assert abs((liq_pair_count - candle_pair_count) / liq_pair_count) < 0.15


def test_liquidity_index_is_datetime(persistent_test_client: Client):
    """Any liquidity samples use datetime index by default.

    Avoid raw running counter indexes. This makes manipulating data much easier.
    """
    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.ethereum, "uniswap-v2")
    pairs = client.fetch_pair_universe()
    pair_universe = LegacyPairUniverse.create_from_pyarrow_table(pairs)
    pair = pair_universe.get_pair_by_ticker_by_exchange(exchange.exchange_id, "WETH", "DAI")

    exchange = exchange_universe.get_by_chain_and_slug(ChainId.ethereum, "uniswap-v2")
    assert exchange, "Uniswap v2 not found"

    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()
    liquidity_universe = GroupedLiquidityUniverse(raw_liquidity_samples)
    liq1 = liquidity_universe.get_liquidity_samples_by_pair(pair.pair_id)
    assert isinstance(liq1.index, pd.DatetimeIndex)


def test_merge_liquidity_samples(persistent_test_client: Client):
    """Merging two liquidity graphs using Pandas should work.


    See also liquidity-analysis.ipynb
    """

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()

    uniswap_v2 = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "uniswap v2")
    sushi_swap = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "sushi")

    raw_pairs = client.fetch_pair_universe().to_pandas()
    raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()

    pair_universe = PandasPairUniverse(raw_pairs, build_index=False)

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
