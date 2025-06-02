"""Candle universe loading tests."""

import datetime

import pandas
import pandas as pd
import pytest
from pandas import Timestamp
from pandas.core.groupby import DataFrameGroupBy

from tradingstrategy.candle import GroupedCandleUniverse, is_candle_green, is_candle_red
from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.reader import read_parquet
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.transport.jsonl import JSONLMaxResponseSizeExceeded
from tradingstrategy.utils.groupeduniverse import resample_candles, resample_dataframe, resample_price_series
from tradingstrategy.utils.wrangle import examine_anomalies, fix_prices_in_between_time_frames, examine_price_between_time_anomalies, fix_dex_price_data, normalise_volume


def test_grouped_candles(persistent_test_client: Client):
    """Group downloaded candles by a trading pair."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    raw_pairs = client.fetch_pair_universe().to_pandas()
    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()

    pair_universe = PandasPairUniverse(raw_pairs, build_index=False)
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
    # assert max_price == pytest.approx(24.120058)

    # Depends on real sushi price, let's give up testing this
    assert min_price == pytest.approx(0.4128446016227597)


def test_empty_candle_universe():
    universe = GroupedCandleUniverse.create_empty()
    assert universe.get_candle_count() == 0


def test_samples_by_timestamp(persistent_test_client: Client):
    """Get all OHLCV candles at a certain timestamp."""

    client = persistent_test_client

    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()
    candle_universe = GroupedCandleUniverse(raw_candles)

    # The internal weekly start before Dec 2021
    ts = pd.Timestamp("2021-11-29")
    candles = candle_universe.get_all_samples_by_timestamp(ts)
    assert len(candles) > 1000
    assert candles.iloc[0].timestamp == ts
    assert candles.iloc[0].open > 0
    assert candles.iloc[0].close > 0


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
        if last_candle["close"] > 0 and first_candle["open"] > 0:
            try:
                momentum = (last_candle["close"] - first_candle["open"]) / first_candle["open"] - 1
            except RuntimeWarning:
                # RuntimeWarning: overflow encountered in float_scalars
                # float23 calculation may overflow for some pairs
                pass


def test_data_for_single_pair(persistent_test_client: Client, default_pairs_df):
    """Get data from the single pair candle universe."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    pairs_df = default_pairs_df

    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")

    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD")]
        )

    pair = pair_universe.get_single()
    assert pair.base_token_symbol == "WBNB"
    assert pair.quote_token_symbol == "BUSD"

    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()

    # Filter down candles to a single pair
    single_pair_candles = raw_candles.loc[raw_candles["pair_id"] == pair.pair_id]

    candle_universe = GroupedCandleUniverse(single_pair_candles)

    # Get last 10 candles for WBNB-BUSD
    df = candle_universe.get_single_pair_data(sample_count=10)
    assert len(df) == 10
    assert df.iloc[-1]["timestamp"] > pd.Timestamp("2021-1-1")


def test_data_for_two_pairs(persistent_test_client: Client, default_pairs_df):
    """Get data from the two pair candle universe."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    pairs_df = default_pairs_df

    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")

    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD"), (exchange.chain_id, exchange.exchange_slug, "Cake", "WBNB")]
        )

    assert pair_universe.get_count() == 2

    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()

    # Filter down candles to two pairs
    two_pair_candles = raw_candles.loc[raw_candles["pair_id"].isin(pair_universe.df)]
    candle_universe = GroupedCandleUniverse(two_pair_candles)


def test_candle_colour(persistent_test_client: Client, default_pairs_df):
    """Green and red candle coloring functions work."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    pairs_df = default_pairs_df

    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")

    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD")]
        )

    pair = pair_universe.get_single()
    assert pair.base_token_symbol == "WBNB"
    assert pair.quote_token_symbol == "BUSD"

    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()

    # Filter down candles to a single pair
    single_pair_candles = raw_candles.loc[raw_candles["pair_id"] == pair.pair_id]

    candle_universe = GroupedCandleUniverse(single_pair_candles)

    # candle = single_pair_candles.loc[pd.Timestamp("2021-04-19")]
    indexed_candles = candle_universe.get_single_pair_data()

    # Handpicked random entry

    # pair_id                      1015916
    # timestamp        2022-02-14 00:00:00
    # exchange_rate                    1.0
    # open                      399.781891
    # close                     380.350555
    # high                      439.315765
    # low                       374.533813
    # buys                          520815
    # sells                         502240
    # buy_volume               246035072.0
    # sell_volume              248500144.0
    # avg                       408.007294
    # start_block                 15233866
    # end_block                   15434838

    candle = indexed_candles.loc[pd.Timestamp("2022-02-14")]
    assert not is_candle_green(candle)
    assert is_candle_red(candle)


def test_candle_upsample(persistent_test_client: Client, default_pairs_df):
    """Upsample OHLCV candles."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    pairs_df = default_pairs_df

    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")

    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD")]
        )

    pair = pair_universe.get_single()
    assert pair.base_token_symbol == "WBNB"
    assert pair.quote_token_symbol == "BUSD"

    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()

    # Filter down candles to a single pair
    single_pair_candles = raw_candles.loc[raw_candles["pair_id"] == pair.pair_id]
    single_pair_candles = single_pair_candles.set_index("timestamp", drop=False)
    monthly_candles = resample_candles(single_pair_candles, pd.Timedelta(days=30))
    assert len(monthly_candles) <= len(single_pair_candles) / 4


def test_candle_resample_and_shift():
    """Resample candles to a higher time frame and shift at the same time."""
    data = {

        'open': [
            100, 100, 100, 100, 100, 100,
            105, 105, 105, 105, 105, 105,
            110, 110, 110, 110, 110, 110,
        ],

        'close': [
            100, 100, 100, 100, 100, 100,
            105, 105, 105, 105, 105, 105,
            110, 110, 110, 110, 110, 110,
        ],
    }
    index = pd.to_datetime([
        '2023-01-01 00:00',
        '2023-01-01 04:00',
        '2023-01-01 08:00',
        '2023-01-01 12:00',
        '2023-01-01 16:00',
        '2023-01-01 20:00',
        #
        '2023-01-02 00:00',
        '2023-01-02 04:00',
        '2023-01-02 08:00',
        '2023-01-02 12:00',
        '2023-01-02 16:00',
        '2023-01-02 20:00',
        #
        '2023-01-03 00:00',
        '2023-01-03 04:00',
        '2023-01-03 08:00',
        '2023-01-03 12:00',
        '2023-01-03 16:00',
        '2023-01-03 20:00',
    ])
    df = pd.DataFrame(data, index=index)
    daily_candles = resample_candles(df, pd.Timedelta(days=1))
    #                open  close  timestamp
    #    2023-01-01   100    100 2023-01-01
    #    2023-01-02   105    105 2023-01-02
    #    2023-01-03   110    110 2023-01-03

    daily_candles_shifted_1 = resample_candles(df, pd.Timedelta(days=1), shift=-1)
    #                 open  close  timestamp
    #    2023-01-01  100.0  105.0 2023-01-01
    #    2023-01-02  105.0  110.0 2023-01-02
    #    2023-01-03  110.0  110.0 2023-01-03

    assert daily_candles.to_dict() == {'open': {Timestamp('2023-01-01 00:00:00'): 100, Timestamp('2023-01-02 00:00:00'): 105, Timestamp('2023-01-03 00:00:00'): 110}, 'close': {Timestamp('2023-01-01 00:00:00'): 100, Timestamp('2023-01-02 00:00:00'): 105, Timestamp('2023-01-03 00:00:00'): 110}, 'timestamp': {Timestamp('2023-01-01 00:00:00'): Timestamp('2023-01-01 00:00:00'), Timestamp('2023-01-02 00:00:00'): Timestamp('2023-01-02 00:00:00'), Timestamp('2023-01-03 00:00:00'): Timestamp('2023-01-03 00:00:00')}}
    assert daily_candles_shifted_1.to_dict() == {'open': {Timestamp('2023-01-01 00:00:00'): 100.0, Timestamp('2023-01-02 00:00:00'): 105.0, Timestamp('2023-01-03 00:00:00'): 110.0}, 'close': {Timestamp('2023-01-01 00:00:00'): 105.0, Timestamp('2023-01-02 00:00:00'): 110.0, Timestamp('2023-01-03 00:00:00'): 110.0}, 'timestamp': {Timestamp('2023-01-01 00:00:00'): Timestamp('2023-01-01 00:00:00'), Timestamp('2023-01-02 00:00:00'): Timestamp('2023-01-02 00:00:00'), Timestamp('2023-01-03 00:00:00'): Timestamp('2023-01-03 00:00:00')}}


def test_price_series_resample_and_shift():
    """Resample price series to a higher time frame and shift at the same time."""
    close_price = [
        100, 100, 100, 100, 100, 100,
        105, 105, 105, 105, 105, 105,
        110, 110, 110, 110, 110, 110,
    ]
    index = pd.to_datetime([
        '2023-01-01 00:00',
        '2023-01-01 04:00',
        '2023-01-01 08:00',
        '2023-01-01 12:00',
        '2023-01-01 16:00',
        '2023-01-01 20:00',
        #
        '2023-01-02 00:00',
        '2023-01-02 04:00',
        '2023-01-02 08:00',
        '2023-01-02 12:00',
        '2023-01-02 16:00',
        '2023-01-02 20:00',
        #
        '2023-01-03 00:00',
        '2023-01-03 04:00',
        '2023-01-03 08:00',
        '2023-01-03 12:00',
        '2023-01-03 16:00',
        '2023-01-03 20:00',
    ])

    series = pd.Series(close_price, index=index)
    daily_close = resample_price_series(series, pd.Timedelta(days=1))
    assert daily_close.to_dict() == {Timestamp('2023-01-01 00:00:00'): 100, Timestamp('2023-01-02 00:00:00'): 105, Timestamp('2023-01-03 00:00:00'): 110}

    series = pd.Series(close_price, index=index)
    daily_close = resample_price_series(series, pd.Timedelta(days=1), shift=-1)
    assert daily_close.to_dict() == {Timestamp('2023-01-01 00:00:00'): 105, Timestamp('2023-01-02 00:00:00'): 110, Timestamp('2023-01-03 00:00:00'): 110}

    series = pd.Series(close_price, index=index)
    daily_open = resample_price_series(series, pd.Timedelta(days=1), shift=-1, price_series_type="open")
    assert daily_open.to_dict() == {Timestamp('2023-01-01 00:00:00'): 100, Timestamp('2023-01-02 00:00:00'): 105, Timestamp('2023-01-03 00:00:00'): 110}


def test_resample_dataframe_and_shift():
    BBU = [
        100, 100, 100, 100, 100, 100,
        105, 105, 105, 105, 105, 105,
        110, 110, 110, 110, 110, 110,
    ]
    index = pd.to_datetime([
        '2023-01-01 00:00',
        '2023-01-01 04:00',
        '2023-01-01 08:00',
        '2023-01-01 12:00',
        '2023-01-01 16:00',
        '2023-01-01 20:00',
        #
        '2023-01-02 00:00',
        '2023-01-02 04:00',
        '2023-01-02 08:00',
        '2023-01-02 12:00',
        '2023-01-02 16:00',
        '2023-01-02 20:00',
        #
        '2023-01-03 00:00',
        '2023-01-03 04:00',
        '2023-01-03 08:00',
        '2023-01-03 12:00',
        '2023-01-03 16:00',
        '2023-01-03 20:00',
    ])
    df = pd.DataFrame({'BBU': BBU}, index=index)
    df['BBM'] = df['BBU'] - 5
    df['BBL'] = df['BBM'] - 5  

    bb_bands_1d=resample_dataframe(df, pd.Timedelta(days=1))

    assert all(bb_bands_1d.columns == df.columns)
    assert(bb_bands_1d.index.freq.delta == pd.Timedelta(days=1))


def test_candle_get_last_entries(persistent_test_client: Client):
    """Get candles before a certain timestamp."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    columnar_pair_table = client.fetch_pair_universe()
    pairs_df = columnar_pair_table.to_pandas()

    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")

    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD")]
        )

    pair = pair_universe.get_single()
    raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()
    candle_universe = GroupedCandleUniverse(raw_candles)

    candles = candle_universe.get_last_entries_by_pair_and_timestamp(
        pair.pair_id,
        pd.Timestamp("2022-01-01"),
    )
    last = candles.iloc[-1]
    assert last["timestamp"] == pd.Timestamp("2021-12-27")
    assert last["pair_id"] == pair.pair_id



@pytest.mark.skip(reason="This test currently downloads a 3.4G parquet and load it to RAM, TODO: move to manual test")
def test_filter_pyarrow(persistent_test_client: Client, default_pairs_df):
    """Filter loaded pyarrow files without loading them fully to the memory.

    Ensures that we can work on candle and liquidity data files on low memory servers.

    .. note ::

        Looks like the current 4h candle dataset peaks at 2.7GB

    .. code-block:: plain

        Do nothing test:  1276.796875 MB
        Load exchanges: 1253.359375 MB
        Load pairs: 1373.125 MB
        Create filtered pair universe: 1408.84375 MB
        Read candles parquet 4h, using single pair filter: 2626.796875 MB
        Convert it to Pandas:  2710.203125 MB
        Using 1h candles:  4725.296875 MB
    """

    # TODO: This test is still experimental

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = default_pairs_df

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD")]
        )

    method = "filtered_parquet"

    # # Load candles for the named pair only
    if method == "filtered_parquet":
        # Load by using Parquet filter functoin
        candle_file = client.fetch_candle_dataset(TimeBucket.h1)
        filter = pair_universe.create_parquet_load_filter()
        pq = read_parquet(candle_file, filter)
        single_pair_candles: pandas.DataFrame = pq.to_pandas()
    else:
        # Load everything to Pandas,
        # then filter down
        df = client.fetch_candle_dataset(TimeBucket.h1).to_pandas()
        pair = pair_universe.get_single()
        single_pair_candles = df.loc[df["pair_id"] == pair.pair_id]

    pair_ids = single_pair_candles["pair_id"].unique()
    assert len(pair_ids) == 1

    #mem_used = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # print(f"Max mem {mem_used/(1024*1024)} MB")


def test_load_candles_using_jsonl(persistent_test_client: Client, default_pairs_df):
    """Load data using JSONL endpoint"""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = default_pairs_df

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD")]
        )

    pair = pair_universe.get_single()
    candles_df = client.fetch_candles_by_pair_ids([pair.pair_id], TimeBucket.h1)

    assert len(candles_df) >= 10_000

    candle_universe = GroupedCandleUniverse(candles_df)

    first_at, last_at = candle_universe.get_timestamp_range()
    assert first_at == pd.Timestamp('2021-04-24 01:00:00')
    assert last_at >= pd.Timestamp('2022-08-10 11:00:00')
    test_price = candle_universe.get_closest_price(pair.pair_id, pd.Timestamp("2022-01-01"))
    assert test_price == pytest.approx(516.9167236844088)



def test_load_candles_using_jsonl_max_bytes(persistent_test_client: Client, default_pairs_df):
    """OverloadJSONL endpoint max_bytes"""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = default_pairs_df

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD")],
        )

    pair = pair_universe.get_single()
    with pytest.raises(JSONLMaxResponseSizeExceeded):
        client.fetch_candles_by_pair_ids(
            [pair.pair_id],
            TimeBucket.h1,
            max_bytes=5_000, # 5kBytes
        )


def test_load_candles_using_json_historical(persistent_test_client: Client, default_pairs_df):
    """Load historical candles using JSONL endpoint"""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = default_pairs_df

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD")],
        )

    pair = pair_universe.get_single()
    candles_df = client.fetch_candles_by_pair_ids(
        {pair.pair_id},
        TimeBucket.h1,
        start_time=datetime.datetime(2023, 1, 1),
        end_time=datetime.datetime(2023, 1, 2)
    )
    assert len(candles_df) == 25  # 24 hours + 1 inclusive


def test_examine_anomalies_single_pair(persistent_test_client: Client, default_pairs_df):
    """Run examine_anomalies() on candle data"""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = default_pairs_df

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [
                (exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD")
            ],
        )

    pair = pair_universe.get_single()
    candles_df = client.fetch_candles_by_pair_ids(
        {pair.pair_id},
        TimeBucket.d1,
        start_time=datetime.datetime(2023, 1, 1),
        end_time=datetime.datetime(2024, 1, 1)
    )

    issues_found = examine_anomalies(
        pair_universe,
        candles_df,
    )
    assert not issues_found


def test_examine_anomalies_multi_pair(persistent_test_client: Client, default_pairs_df):
    """Run examine_anomalies() on candle data for multiple pairs"""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = default_pairs_df

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [
                (exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD"),
                (exchange.chain_id, exchange.exchange_slug, "Cake", "BUSD")
            ],
        )

    pairs = {pair.pair_id for pair in pair_universe.iterate_pairs()}
    candles_df = client.fetch_candles_by_pair_ids(
        pairs,
        TimeBucket.d1,
        start_time=datetime.datetime(2023, 1, 1),
        end_time=datetime.datetime(2024, 1, 1)
    )
    assert len(candles_df["pair_id"].unique()) == 2

    issues_found = examine_anomalies(
        pair_universe,
        candles_df,
    )
    assert not issues_found


def test_fix_prices_in_between_time_frames_no_actions(persistent_test_client: Client, default_pairs_df):
    """Run fix_prices_in_between_time_frames() - nothing should happen"""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = default_pairs_df

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [
                (exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD"),
                (exchange.chain_id, exchange.exchange_slug, "Cake", "BUSD")
            ],
        )

    pairs = {pair.pair_id for pair in pair_universe.iterate_pairs()}
    candles_df = client.fetch_candles_by_pair_ids(
        pairs,
        TimeBucket.d1,
        start_time=datetime.datetime(2023, 1, 1),
        end_time=datetime.datetime(2024, 1, 1)
    )
    assert len(candles_df["pair_id"].unique()) == 2

    candles_df = candles_df.set_index("timestamp", drop=False)
    candles_dfgb = candles_df.groupby("pair_id")

    healed_candles_dfgb = fix_prices_in_between_time_frames(
        candles_dfgb,
    )

    # No changes in open/close
    for pair_id in candles_df["pair_id"].unique():
        for column in ("open", "close"):
            original = candles_dfgb.get_group(pair_id)[column]
            healed = healed_candles_dfgb.get_group(pair_id)[column]
            assert original.equals(healed)


def test_fix_prices_in_between_time_frames_broken_data(persistent_test_client: Client, default_pairs_df):
    """Run fix_prices_in_between_time_frames().

    - Fix one broken entry we create

    - See broken-price-data.ipynb
    """

    client = persistent_test_client
    pairs_df = default_pairs_df

    # Create filtered exchange and pair data
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [
                (ChainId.ethereum, "uniswap-v3", "MEME", "WETH"),
            ],
        )

    pairs = [pair.pair_id for pair in pair_universe.iterate_pairs()]
    candles_df = client.fetch_candles_by_pair_ids(
        pairs,
        TimeBucket.h1,
        start_time=datetime.datetime(2024, 9, 25),
        end_time=datetime.datetime(2024, 10, 1)
    )
    assert len(candles_df["pair_id"].unique()) == 1

    # Break one value in one pair at certain timestamp
    broken_pair_id = pairs[0]
    # candles_df.loc[(candles_df["timestamp"] == pd.Timestamp("2023-01-02")) & (candles_df["pair_id"] == broken_pair_id), "open"] = 0.001

    candles_df = candles_df.set_index("timestamp", drop=False)
    candles_dfgb = candles_df.groupby("pair_id")

    # We check we have one broken value
    anomalies = examine_price_between_time_anomalies(
        candles_dfgb.get_group(broken_pair_id)["open"],
    )
    assert len(anomalies) == 3

    healed_candles_dfgb = fix_prices_in_between_time_frames(
        candles_dfgb,
    )

    # One healing change spotted in open price series
    original = candles_dfgb.get_group(broken_pair_id)["open"]
    healed = healed_candles_dfgb.get_group(broken_pair_id)["open"]
    assert not original.equals(healed)

    # Check MEV rigged price was replaced away
    assert healed["2024-09-27 10:00"] == pytest.approx(0.012104, rel=0.10)


def test_fix_min_max_price(persistent_test_client: Client, default_pairs_df):
    """Run remove_min_max_price().

    - Fix one broken entry we create
    """

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = default_pairs_df

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [
                (exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD"),
                (exchange.chain_id, exchange.exchange_slug, "Cake", "BUSD")
            ],
        )

    pairs = [pair.pair_id for pair in pair_universe.iterate_pairs()]
    candles_df = client.fetch_candles_by_pair_ids(
        pairs,
        TimeBucket.d1,
        start_time=datetime.datetime(2023, 1, 1),
        end_time=datetime.datetime(2024, 1, 1)
    )

    candles_df = candles_df.set_index("timestamp", drop=False)
    candles_dfgb = candles_df.groupby("pair_id")

    # Calls remove_min_max_price() internally
    df = fix_dex_price_data(
        candles_dfgb,
        freq="D",
    )
    # We have nothing to check ATM
    # TODO: Manually inject a broken value
    assert isinstance(df, DataFrameGroupBy)


def test_normalise_volume(persistent_test_client: Client, default_pairs_df):
    """Normalise volume data ulocaniswap v2 + v3."""

    client = persistent_test_client
    pairs_df = default_pairs_df

    # Create filtered exchange and pair data
    pair_universe = PandasPairUniverse.create_pair_universe(
        pairs_df,
        [
            (ChainId.ethereum, "uniswap-v2", "WETH", "USDC", 0.0030),
            (ChainId.ethereum, "uniswap-v3", "WETH", "USDC", 0.0005),
        ],
    )

    pairs = [pair.pair_id for pair in pair_universe.iterate_pairs()]
    candles_df = client.fetch_candles_by_pair_ids(
        pairs,
        TimeBucket.d1,
        start_time=datetime.datetime(2024, 1, 1),
        end_time=datetime.datetime(2024, 2, 1)
    )

    candles_df = candles_df.set_index("timestamp", drop=False)
    assert len(candles_df) == 64

    # Fix volume columns
    volume_fixed_candles_dfgb = normalise_volume(candles_df)

    # Group by pair, check we have good data
    candles_dfgb = volume_fixed_candles_dfgb.groupby("pair_id")
    for pair in pair_universe.iterate_pairs():
        candles = candles_dfgb.get_group(pair.pair_id)
        assert candles["volume"].sum() > 0

    df = fix_dex_price_data(
        candles_dfgb,
        freq="D",
    )
    # We have nothing to check ATM
    # TODO: Manually inject a broken value
    assert isinstance(df, DataFrameGroupBy)
