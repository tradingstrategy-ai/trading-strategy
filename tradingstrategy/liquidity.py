"""Liquidity data feed manipulation.

For more information about liquidity in automatic market making pools see :term:`AMM`
and :term:`XY liquidity model`.
"""

import datetime
from dataclasses import dataclass
from typing import List, Optional, Iterable, Tuple, Dict

import pandas as pd
import pyarrow as pa
from dataclasses_json import dataclass_json
from pandas.core.groupby import GroupBy, DataFrameGroupBy

from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.types import UNIXTimestamp, USDollarAmount, BlockNumber, PrimaryKey
from tradingstrategy.utils.groupeduniverse import PairGroupedUniverse


# Preconstructed pd.Tiemdelta for optimisation
_ZERO_TIMEDELTA = pd.Timedelta(0)


class LiquidityDataUnavailable(Exception):
    """We tried to look up liquidity info for a trading pair, but count not find a sample."""


@dataclass_json
@dataclass
class XYLiquidity:
    """Data structure that presents liquidity status in bonding curve pool.

    This data structure is for naive x*y=k :term:`AMM` pool.
    Liquidity is not the part of the normal :term:`technical analysis`,
    so the dataset server has separate datasets for it.

    Liquidity is expressed as US dollar value of the :term:`quote token` of the pool.
    For example if the pool is 50 $FOO in reserve0 and 50 $USDC in reserve1, the
    liquidity of the pool would be expressed as 50 USD.

    Liquidity events, like :term:`candles <candle>`, have open, high, low and close values,
    depending on which time of the candle they were sampled.
    """

    #: Primary key to identity the trading pair
    #: Use pair universe to map this to chain id and a smart contract address
    pair_id: PrimaryKey

    #: Open timestamp for this time bucket.
    timestamp: UNIXTimestamp

    #: USD exchange rate of the quote token used to
    #: convert to dollar amounts in this time bucket.
    #:
    #: Note that currently any USD stablecoin (USDC, DAI) is
    #: assumed to be 1:1 and the candle server cannot
    #: handle exchange rate difference among stablecoins.
    #:
    #: The rate is taken at the beginning of the 1 minute time bucket.
    #: For other time buckets, the exchange rate is the simple average
    #: for the duration of the bucket.
    exchange_rate: float

    #: Liquidity absolute values in the pool in different time points.
    #: Note - for minute candles - if the candle contains only one event (mint, burn, sync)
    #: the open liquidity value is the value AFTER this event.
    #: The dataset server does not track the closing value of the previous liquidity event.
    #: This applies for minute candles only.
    open: USDollarAmount

    #: Liquidity absolute values in the pool in different time points
    close: USDollarAmount

    #: Liquidity absolute values in the pool in different time points
    high: USDollarAmount

    #: Liquidity absolute values in the pool in different time points
    low: USDollarAmount

    #: Number of liquidity supplied events for pool
    adds: int

    #: Number of liquidity removed events for the pool
    removes: int

    #: Number of total events affecting liquidity during the time window.
    #: This is adds, removes AND swaps AND sync().
    syncs: int

    #: How much new liquidity was supplied, in the terms of the quote token converted to US dollar
    add_volume: USDollarAmount

    #: How much new liquidity was removed, in the terms of the quote token converted to US dollar
    add_volume: USDollarAmount

    #: Blockchain tracking information
    start_block: BlockNumber

    #: Blockchain tracking information
    end_block: BlockNumber

    def __repr__(self):
        human_timestamp = datetime.datetime.utcfromtimestamp(self.timestamp)
        return f"@{human_timestamp} O:{self.open} H:{self.high} L:{self.low} C:{self.close} V:{self.volume} A:{self.adds} R:{self.removes} SB:{self.start_block} EB:{self.end_block}"

    @classmethod
    def to_pyarrow_schema(cls, small_candles=False) -> pa.Schema:
        """Construct schema for writing Parquet filess for these candles.

        :param small_candles: Use even smaller word sizes for frequent (1m) candles.
        """
        schema = pa.schema([
            ("pair_id", pa.uint32()),
            ("timestamp", pa.timestamp("s")),
            ("exchange_rate", pa.float32()),
            ("open", pa.float32()),
            ("close", pa.float32()),
            ("high", pa.float32()),
            ("low", pa.float32()),
            ("adds", pa.uint16() if small_candles else pa.uint32()),
            ("removes", pa.uint16() if small_candles else pa.uint32()),
            ("syncs", pa.uint16() if small_candles else pa.uint32()),
            ("add_volume", pa.float32()),
            ("remove_volume", pa.float32()),
            ("start_block", pa.uint32()),   # Should we good for 4B blocks
            ("end_block", pa.uint32()),
        ])
        return schema

    @classmethod
    def to_dataframe(cls) -> pd.DataFrame:
        """Return emptry Pandas dataframe presenting liquidity sample."""

        # TODO: Does not match the spec 1:1 - but only used as empty in tests
        fields = dict([
            ("pair_id", "int"),
            ("timestamp", "datetime64[s]"),
            ("exchange_rate", "float"),
            ("open", "float"),
            ("close", "float"),
            ("high", "float"),
            ("low", "float"),
            ("buys", "float"),
            ("sells", "float"),
            ("add_volume", "float"),
            ("remove_volume", "float"),
            ("start_block", "float"),
            ("end_block", "float"),
        ])
        df = pd.DataFrame(columns=fields.keys())
        return df.astype(fields)


@dataclass_json
@dataclass
class LiquidityResult:
    """Server-reply for live queried liquidity data."""

    #: A bunch of candles.
    #: Candles are unordered and subject to client side sorting.
    #: Multiple pairs and chains may be present in candles.
    liquidity_events: List[XYLiquidity]

    def sort_by_timestamp(self):
        """In-place sorting of candles by their timestamp."""
        self.candles.sort(key=lambda c: c.timestamp)


class GroupedLiquidityUniverse(PairGroupedUniverse):
    """A universe where each trading pair has its own liquidity data feed.

    This is helper class to create foundation for multi pair strategies.

    For the data logistics purposes, all candles are lumped together in single columnar data blobs.
    However, it rarely makes sense to execute operations over different trading pairs.
    :py:class`GroupedLiquidityUniverse` creates trading pair id -> liquidity sample data grouping out from
    raw liquidity sample.
    """

    def __init__(self,
                 df: pd.DataFrame,
                 time_bucket=TimeBucket.d1,
                 timestamp_column="timestamp",
                 index_automatically=True):
        super().__init__(df, time_bucket, timestamp_column, index_automatically)

        # For fast liquidity lookup
        self.indexer_cache: Dict[PrimaryKey, int] = {}

    def get_liquidity_samples_by_pair(self, pair_id: PrimaryKey) -> Optional[pd.DataFrame]:
        """Get samples for a single pair.

        If the pair does not exist return `None`.
        """
        try:
            return self.get_samples_by_pair(pair_id)
        except KeyError:
            return None

    def get_liquidity_with_tolerance(self,
                          pair_id: PrimaryKey,
                          when: pd.Timestamp,
                          tolerance: pd.Timedelta,
                          kind="close") -> Tuple[USDollarAmount, pd.Timedelta]:
        """Get the available liquidity for a trading pair at a specific time point/

        The liquidity is defined as one-sided as in :term:`XY liquidity model`.

        Example:

        .. code-block:: python

            liquidity_amount, last_trade_delay = liquidity_universe.get_liquidity_with_tolerance(
            sushi_usdt.pair_id,
                pd.Timestamp("2021-12-31"),
                tolerance=pd.Timedelta("1y"),
            )
            assert liquidity_amount == pytest.approx(2292.4517)
            assert last_trade_delay == pd.Timedelta('4 days 00:00:00')

        :param pair_id:
            Trading pair id

        :param when:
            Timestamp to query

        :param kind:
            One of OHLC data points: "open", "close", "low", "high"

        :param tolerance:
            If there is no liquidity sample available at the exact timepoint,
            look to the past to the get the nearest sample.
            For example if candle time interval is 5 minutes and look_back_timeframes is 10,
            then accept a candle that is maximum of 50 minutes before the timepoint.

        :return:
            Return (price, delay) tuple.

            We always return a price. In the error cases an exception is raised.
            The delay is the timedelta between the wanted timestamp
            and the actual timestamp of the candle.

            Candles are always timestamped by their opening.

        :raise LiquidityDataUnavailable:

            There were no samples available with the given condition.

            This can happen when

            - There has not been a single trade yet

            - There hasn't been any trades since `tolerance`
              time window

        """

        assert kind in ("open", "close", "high", "low"), f"Got kind: {kind}"

        last_allowed_timestamp = when - tolerance

        try:
            candles_per_pair = self.get_samples_by_pair(pair_id)
        except KeyError as e:
            raise LiquidityDataUnavailable(f"Pair {pair_id} does not contain any liquidity samples at all") from e

        samples_per_kind = candles_per_pair[kind]

        # https://pandas.pydata.org/docs/reference/api/pandas.Index.get_indexer.html
        # https://stackoverflow.com/questions/71027193/datetimeindex-get-loc-is-deprecated
        indexer = candles_per_pair.index.get_indexer([when], method="pad")

        if indexer[0] != -1:
            discrete_ts_idx = indexer[0]
        else:
            first_sample = candles_per_pair.index[0]
            raise LiquidityDataUnavailable(f"Pair {pair_id} does not have data older data than {first_sample}. Tried to look up {when}.")

        sample_timestamp = candles_per_pair.index[discrete_ts_idx]

        latest_value = candles_per_pair.iloc[discrete_ts_idx][kind]

        distance = when - sample_timestamp
        assert distance >= _ZERO_TIMEDELTA, f"Somehow we managed to get a timestamp {sample_timestamp} that is newer than asked {when}"

        if sample_timestamp >= last_allowed_timestamp:
            # Return the chosen price column of the sample
            return latest_value, distance

        # Try to be helpful with the errors here,
        # so one does not need to open ipdb to inspect faulty data
        try:
            first_sample = candles_per_pair.iloc[0]
            second_sample = candles_per_pair.iloc[1]
            last_sample = candles_per_pair.iloc[-1]
        except KeyError:
            raise LiquidityDataUnavailable(
                f"Could not find any liquidity samples for pair {pair_id}, value kind '{kind}', between {when} - {last_allowed_timestamp}\n"
                f"Could not figure out existing data range. Has {len(samples_per_kind)} samples."
            )

        raise LiquidityDataUnavailable(
            f"Could not find any liquidity samples for pair {pair_id}, value kind '{kind}', between {when} - {last_allowed_timestamp}\n"
            f"The pair has {len(samples_per_kind)} candles between {first_sample['timestamp']} - {last_sample['timestamp']}\n"
            f"Sample interval is {second_sample['timestamp'] - first_sample['timestamp']}\n"
            f"\n"
            f"This is usually due to sparse candle data - trades have not been made or the blockchain was halted during the price look-up period.\n"
            f"Try to increase look back perid in your code."
            )

    def get_closest_liquidity(self, pair_id: PrimaryKey, when: pd.Timestamp, kind="open", look_back_time_frames=5) -> USDollarAmount:
        """Get the available liuqidity for a trading pair at a specific timepoint or some candles before the timepoint.

        The liquidity is defined as one-sided as in :term:`XY liquidity model`.

        .. warning::

                This is an early alpha method and has been deprecated.
                Please use  :py:meth:`get_liquidity_with_tolerance` instead.

        :param pair_id: Traing pair id
        :param when: Timestamp to query
        :param kind: One of liquidity samples: "open", "close", "low", "high"
        :param look_back_timeframes: If there is no liquidity sample available at the exact timepoint,
            look to the past to the get the nearest sample
        :return: We always return
        :raise LiquidityDataUnavailable: There was no liquidity sample available
        """

        assert kind in ("open", "close", "high", "low"), f"Got kind: {kind}"

        start_when = when
        samples_per_pair = self.get_liquidity_samples_by_pair(pair_id)
        assert samples_per_pair is not None, f"No liquidity data available for pair {pair_id}"

        samples_per_kind = samples_per_pair[kind]
        for attempt in range(look_back_time_frames):
            try:
                sample = samples_per_kind[when]
                return sample
            except KeyError:
                # Go to the previous sample
                when -= self.time_bucket.to_timedelta()

        raise LiquidityDataUnavailable(f"Could not find any liquidity samples for pair {pair_id} between {when} - {start_when}")

    @staticmethod
    def create_empty() -> "GroupedLiquidityUniverse":
        """Create a liquidity universe without any data."""
        return GroupedLiquidityUniverse(df=XYLiquidity.to_dataframe(), index_automatically=False)


class ResampledLiquidityUniverse:
    """Backtesting performance optimised version of liquidity universe.

    - The class is designed to be used with strategies that trade
      a large number of pairs and picks pairs by their liquidity criteria,
      which varies over time

    - Because liquidity information does not need to be as accurate as pricing information,
      and a rough estimate of liquidity is enough for most strategies, we can preconstruct
      an optimised version of liquidity universe

    - This version of liquidity universe is designed for the maximum speed of
      :py:meth:`get_liquidity_fast` with (pair id, timestamp) parameter

    - Some of the speed is achieved by precomputing upsampling liquidity data
      to a longer frequency (day, week)

    - Some of the speed is achieved by using extra indexes and memory

    - Some of the speed is achieved by forward filling any data gaps,
      so we do not need to worry about the sparse data

    The speedup difference between :py:meth`LiquidityUniverse.get_liquidity_with_tolerance`
    and py:meth:`get_liquidity_fast` is more than 10x.
    The practice speed increase for multipair liquidity aware strategy backtest can be
    2x - 3x.

    .. note ::

        The resampling itself will take some time, so optimally
        the resampled data should be stored on-disk cache between different backtest runs

    """

    def __init__(self,
                df: pd.DataFrame,
                column="close",
                resample_period="1D",
                resample_method="min",
        ):
        """Calculate optimised liquidity universe.

        - Only extract the column we are interested in

        - Resample to a smaller  data

        - Resample by the

        .. note::

            For a large number of pairs, resampling will take a long time.
            You should remove any unnecessary pairs in `df` before creating a resampled version.

        :param df:

            Liquidity dataframe.

            You should prepare this by dropping any unwanted pairs.

        :param column:
            Which column value we use for the resampled series.

            Defaults to (daily) close liquidity value.

        :param resample_period:

            Pandas frequency alias string for the new timeframe duration.

            E.g. `1D` for daily.

            Must be a fixed frequency e.g. `30D` instead of `M`.

            See https://stackoverflow.com/a/35339226/315168

        :param resample_method:
            How to we resample the liquidity

        """
        new_df = df[["timestamp", "pair_id"]].copy()
        new_df["value"] = df[[column]]
        new_df.set_index(new_df["timestamp"], inplace=True, drop=True)
        grouped_df = new_df.groupby("pair_id")

        # https://pandas.pydata.org/docs/reference/api/pandas.core.groupby.DataFrameGroupBy.resample.html
        #
        # Important to set origin="epoch" here, or otherwise
        # pd.Timestamp().floor() does not match our range values in get_liquidity_fast()
        #
        resampled_df = grouped_df.resample(resample_period, origin="epoch").agg({"value": resample_method}).ffill()
        self.resample_period = resample_period
        self.df: pd.DataFrame = resampled_df

        self.pair_cache: Dict[PrimaryKey, pd.DataFrame] = {}

    def get_samples_by_pair(self, pair_id: int) -> pd.DataFrame:
        """Access and cache the resampled liquidity data per pair."""
        # https://stackoverflow.com/a/45563615/315168

        if pair_id not in self.pair_cache:
            try:
                self.pair_cache[pair_id] = self.df.xs(pair_id)
            except KeyError as e:
                raise KeyError(f"Could not find pair for pair_id {pair_id}") from e
        return self.pair_cache[pair_id]

    def get_liquidity_fast(self,
                      pair_id: PrimaryKey,
                      when: pd.Timestamp) -> USDollarAmount:
        """Get the available liquidity for a trading pair at a specific time point.

        The liquidity is read from an upsampled buffer.

        The lookup is fast because any timestamp is rounded down to the nearest frequency full timestamp
        and looked up there.

        :param pair_id:
            Trading pair id

        :param when:
            Timestamp to query

        :return:
            Return available liquidty or `0` if no liquidity was seen before the timestamp.
        """
        assert isinstance(when, pd.Timestamp)

        try:
            samples = self.get_samples_by_pair(pair_id)
        except KeyError as e:
            raise LiquidityDataUnavailable(f"No liquidity data for {pair_id}") from e

        try:
            rounded_ts = when.floor(self.resample_period)
            return samples.loc[rounded_ts]["value"]
        except KeyError:
            return 0.0
