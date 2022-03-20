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
    sushi_swap = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "sushi")
    sushi_usdt = pair_universe.get_one_pair_from_pandas_universe(sushi_swap.exchange_id, "SUSHI", "USDT")
    assert sushi_usdt.get_trading_pair_page_url() == "https://tradingstrategy.ai/trading-view/ethereum/sushi/sushi-usdt"
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


def test_empty_candle_universe():
    universe = GroupedCandleUniverse.create_empty()
    assert universe.get_candle_count() == 0


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


def test_samples_by_timestamp_range(persistent_test_client: Client):
    """Get samples for multiple pairs by range."""

    client = persistent_test_client

    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()
    candle_universe = GroupedCandleUniverse(raw_candles)

    # Calibrate our week
    random_date = pd.Timestamp("2021-10-29")
    end = candle_universe.get_prior_timestamp(random_date)

    assert end == pd.Timestamp("2021-10-25")

    # Because we ar using weekly candles,
    # and start and end are inclusive endpoints,
    # we should get 3 weeks of samples
    start = pd.Timestamp(end) - pd.Timedelta(weeks=2)

    # there is one week between the start and the end
    middle = start + (end - start) / 2

    candles = candle_universe.get_all_samples_by_range(start, end)

    # We have pair data for 3 different weeks
    assert len(candles.index.unique()) == 3

    # Each week has its of candles broken down by a pair
    # and can be unique addressed by their pair_id
    assert len(candles.loc[start]) >= 1000
    assert len(candles.loc[middle]) >= 1000
    assert len(candles.loc[end]) >= 1000


def test_iterate_pairs_by_timestamp_range(persistent_test_client: Client):
    """Iterate pairs candles by given timestamp range."""

    client = persistent_test_client

    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()
    candle_universe = GroupedCandleUniverse(raw_candles)

    # Calibrate our week
    random_date = pd.Timestamp("2021-10-29")
    end = candle_universe.get_prior_timestamp(random_date)

    assert end == pd.Timestamp("2021-10-25")

    # Because we ar using weekly candles,
    # and start and end are inclusive endpoints,
    # we should get 3 weeks of samples
    start = pd.Timestamp(end) - pd.Timedelta(weeks=2)

    for pair_id, pair_df in candle_universe.iterate_samples_by_pair_range(start, end):
        # Because of missing samples, some pairs may have different ranges.
        # In this example, we iterate 3 weeks ranges, so we can have data for
        # 1, 2 or 3 weeks.
        # If there was no data at all pair_id is not present in the result.
        range_start = pair_df.index[0]
        range_end = pair_df.index[-1]
        assert range_start <= range_end
        # Calculate the momentum for the full range of all samples
        first_candle = pair_df.iloc[0]
        last_candle = pair_df.iloc[-1]
        # Calculate
        momentum = (last_candle["close"] - first_candle["open"]) / first_candle["open"] - 1
