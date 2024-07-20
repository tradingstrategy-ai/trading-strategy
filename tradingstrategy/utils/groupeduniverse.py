"""Creation of per-trading pair Pandas dataframes.

- Base classes for handling OHLCV data for price and liquidity

- Group raw multipair :py:class:`pandas.DataFrame` to grouped format,
  so that reading and indexing individual trading pair data is easy
  in multipair trading universe

See also

- :py:mod:`tradingstrategy.candle`

- :py:mod:`tradingstrategy.liquidity`

"""

import datetime
import logging
import warnings
from itertools import islice
from typing import Optional, Tuple, Iterable, cast

import numpy as np
import pandas as pd
from pandas import MultiIndex

from tradingstrategy.pair import DEXPair
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.types import PrimaryKey
from tradingstrategy.utils.forward_fill import forward_fill
from tradingstrategy.utils.forward_fill import forward_fill as _forward_fill
from tradingstrategy.utils.time import assert_compatible_timestamp, ZERO_TIMEDELTA, naive_utcnow

logger = logging.getLogger(__name__)


class NoDataAvailable(Exception):
    """Raises when the user is asking data that is empty."""


class PairCandlesMissing(KeyError):
    """Raises when the user is asking data for a pair that has no OHLCV feed."""


class PairGroupedUniverse:
    """A base class for manipulating columnar price/liquidity data by a pair.

    The server streams the data for all pairs in a single continuous time-indexed format.
    For most the use cases, we want to look up and manipulate data by pairs.
    To achieve this, we use Pandas :py:class:`pd.GroupBy` and
    recompile the data on the client side.

    This works for

    - OHLCV candles

    - Liquidity candles

    - Lending reserves (one PairGroupedUniverse per each metric like supply APR and borrow APR)

    The input :py:class:`pd.DataFrame` is sorted by default using `timestamp`
    column and then made this column as an index. This is not optimised (not inplace).

    See also

    - :py:mod:`tradingstrategy.candle`

    - :py:mod:`tradingstrategy.liquidity`
    """

    def __init__(
        self,
        df: pd.DataFrame,
        time_bucket:TimeBucket=TimeBucket.d1,
        timestamp_column: str="timestamp",
        index_automatically: bool=True,
        fix_wick_threshold: tuple | None = (0.1, 1.9),
        primary_key_column: str="pair_id",
        remove_candles_with_zero: bool = True,
        forward_fill: bool = False,
        bad_open_close_threshold: float | None=3.0,
    ):
        """Set up new candle universe where data is grouped by trading pair.

        :param df:
            DataFrame backing the data.

        :param time_bucket:
            What bar size candles we are operating at. Default to daily.

            TODO: Currently not used. Will be removed in the future versions.

        :param timestamp_column:
            What column use to build a time index. Used for QStrader / Backtrader compatibility.

        :param index_automatically:
            Convert the index to use time series. You might avoid this with QSTrader kind of data.

        :param fix_wick_threshold:
            Apply abnormal high/low wick fix filter.

            Percent value of maximum allowed high/low wick relative to close.
            By default fix values where low is 90% lower than close and high is 90% higher than close.

            See :py:func:`~tradingstrategy.utils.groupeduniverse.fix_bad_wicks` for more information.

        :param bad_open_close_threshold:
            See :py:func:`fix_bad_wicks`.

        :param primary_key_column:
            The pair/reserve id column name in the dataframe.
            
        :param remove_zero_candles:
            Remove candles with zero values for OHLC.

            To deal with abnormal data.

        :param forward_fill:
            Forward-will gaps in the data.

            See :term:`forward fill` and :ref:`forward filling data` for more information.
        """
        self.index_automatically = index_automatically
        assert isinstance(df, pd.DataFrame)

        self.timestamp_column = timestamp_column
        self.time_bucket = time_bucket
        self.primary_key_column = primary_key_column

        # TODO: sometimes we crop data, but not index
        # e.g. if we check for good index here test_visualise_strategy_state_overriden_pairs
        # stops working, as the visualisation figure expects timestamp column
        # and not isinstance(df.index, pd.DatetimeIndex)
        if index_automatically:
            # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sort_index.html
            self.df = df \
                .set_index(timestamp_column, drop=False)\
                .sort_index(inplace=False)
        else:
            self.df = df

        if fix_wick_threshold or bad_open_close_threshold:
            self.df = fix_bad_wicks(
                self.df,
                fix_wick_threshold,
                bad_open_close_threshold=bad_open_close_threshold,
            )
            
        if remove_candles_with_zero:
            self.df = remove_zero_candles(self.df)
        
        self.pairs: pd.GroupBy = self.df.groupby(by=self.primary_key_column)

        if forward_fill:
            old_pairs = self.pairs
            self.pairs = _forward_fill(
                self.pairs,
                freq=self.time_bucket.to_frequency(),
            )

        self.candles_cache: dict[int, pd.DataFrame] = {}

    def clear_cache(self):
        """Clear candles cached by pair."""
        self.candles_cache = {}

    def get_columns(self) -> pd.Index:
        """Get column names from the underlying pandas.GroupBy object"""
        return self.pairs.obj.columns

    def get_sample_count(self) -> int:
        """Return the dataset size - how many samples total for all pairs"""
        return len(self.df)

    def get_pair_count(self) -> int:
        """Return the number of pairs in this dataset.

        TODO: Rename. Also used by lending reserves, and this then
        refers to count of reserves, not pairs.
        """
        return len(self.pairs.groups)

    def get_samples_by_pair(self, pair_id: PrimaryKey) -> pd.DataFrame:
        """Get samples for a single pair.

        After the samples have been extracted, set `timestamp` as the index for the data.

        :return:
            Data frame group

        :raise KeyError:
            If we do not have data for pair_id
        """
        try:
            pair = self.pairs.get_group(pair_id)
        except KeyError as e:
            raise PairCandlesMissing(f"No OHLC samples for pair id {pair_id} in {self}") from e
        return pair

    def get_last_entries_by_pair_and_timestamp(self,
            pair: DEXPair | PrimaryKey,
            timestamp: pd.Timestamp | datetime.datetime,
            small_time=pd.Timedelta(seconds=1),
        ) -> pd.DataFrame:
        """Get samples for a single pair before a timestamp.

        Return a DataFrame slice containing all datapoints before the timestamp.

        We assume `timestamp` is current decision frame.
        E.g. for daily close data return the previous day close
        to prevent any lookahead bias.

        :param pair_id:
            Integer id for a trading pair

        :param timestamp:
            Get all samples excluding this timestamp.

        :return:
            Dataframe that contains samples for a single trading pair.

            Indexed by timestamp.

        :raise KeyError:
            If we do not have data for pair_id
        """

        if type(timestamp) == datetime.datetime:
            timestamp = pd.Timestamp(timestamp)

        if isinstance(pair, DEXPair):
            pair_id = pair.pair_id
        elif type(pair) == int:
            pair_id = pair
        else:
            raise AssertionError(f"Unknown pair id {type(pair)}: {pair} passed to get_last_entries_by_pair_and_timestamp(). Make sure you use pair.internal_id integer if unsure.")

        pair_candles = self.get_samples_by_pair(pair_id)
        # Watch out for inclusive timestamp
        # https://stackoverflow.com/questions/49962417/why-does-loc-have-inclusive-behavior-for-slices
        adjusted_timestamp = timestamp - small_time
        return pair_candles.loc[:adjusted_timestamp]

    def get_all_pairs(self) -> Iterable[Tuple[PrimaryKey, pd.DataFrame]]:
        """Go through all liquidity samples, one DataFrame per trading pair."""
        for pair_id, data in self.pairs:
            yield pair_id, data

    def get_pair_ids(self) -> Iterable[PrimaryKey]:
        """Get all pairs present in the dataset"""
        with warnings.catch_warnings():
            # FutureWarning: In a future version of pandas, a length 1 tuple will be returned when 
            # iterating over a groupby with a grouper equal to a list of length 1. 
            # Don't supply a list with a single grouper to avoid this warning.
            
            warnings.simplefilter("ignore")
        
            for pair_id, data in self.pairs:
                yield int(pair_id)

    def get_all_samples_by_timestamp(self, ts: pd.Timestamp) -> pd.DataFrame:
        """Get list of candles/samples for all pairs at a certain timepoint.

        :raise KeyError: The universe does not contain a sample for a given timepoint
        :return: A DataFrame that contains candles/samples at the specific timeout
        """
        assert_compatible_timestamp(ts)
        samples = self.df.loc[self.df[self.timestamp_column] == ts]
        return samples

    def get_all_samples_by_range(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """Get list of candles/samples for all pairs at a certain range.

        Useful to get the last few samples for multiple pairs.

        Example:

        .. code-block:: python

                # Set up timestamps for 3 weeks range, one week in middle
                end = Timestamp('2021-10-25 00:00:00')
                start = Timestamp('2021-10-11 00:00:00')
                middle = start + (end - start) / 2

                # Get weekly candles
                raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()
                candle_universe = GroupedCandleUniverse(raw_candles)
                candles = candle_universe.get_all_samples_by_range(start, end)

                # We have pair data for 3 different weeks
                assert len(candles.index.unique()) == 3

                # Each week has its of candles broken down by a pair
                # and can be unique addressed by their pair_id
                assert len(candles.loc[start]) >= 1000
                assert len(candles.loc[middle]) >= 1000
                assert len(candles.loc[end]) >= 1000

        :param start: start of the range (inclusive)
        :param end: end of the range (inclusive)
        :return: A DataFrame that contains candles/samples for all pairs at the range.
        """
        assert_compatible_timestamp(start)
        assert_compatible_timestamp(end)
        assert start < end, f"Got reverse timestamp range {start} - {end}"

        # https://stackoverflow.com/a/69605701/315168
        samples = self.df.loc[
            (self.df.index >= start) &
            (self.df.index <= end)
        ]
        return samples

    def iterate_samples_by_pair_range(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """Get list of candles/samples for all pairs at a certain range.

        Useful to get the last few samples for multiple pairs.

        Example:

        .. code-block:: python

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
                # In this example, we iterate 3 weeks ranges, so we can have
                # 1, 2 or 3 weekly candles.
                # If there was no data at all pair_id is not present in the result.
                range_start = pair_df.index[0]
                range_end = pair_df.index[-1]
                assert range_start <= range_end
                # Calculate the momentum for the full range of all samples
                first_candle = pair_df.iloc[0]
                last_candle = pair_df.iloc[-1]
                # Calculate
                momentum = (last_candle["close"] - first_candle["open"]) / first_candle["open"] - 1

        :param start: start of the range (inclusive)

        :param end: end of the range (inclusive)

        :return: `DataFrame.groupby` result
        """
        samples = self.get_all_samples_by_range(start, end)
        return samples.groupby(self.primary_key_column)

    def get_timestamp_range(self, use_timezone=False) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        """Return the time range of data we have for.

        .. note ::

            Because we assume multipair data, the data is grouped by and not indexed as time series.
            Thus, this function can be a slow operation.

        :param use_timezone:
            The resulting timestamps will have their timezone set to UTC.
            If not set then naive timestamps are generated.

            Legacy option. Do not use.

        :return:
            (start timestamp, end timestamp) tuple, UTC-timezone aware

            If the data frame is empty, return `None, None`.
        """

        if len(self.df) == 0:
            return None, None

        if(self.index_automatically == True):
            if use_timezone:
                start = (self.df[self.timestamp_column].iat[0]).tz_localize(tz='UTC')
                end = (self.df[self.timestamp_column].iat[-1]).tz_localize(tz='UTC')
            else:
                start = self.df[self.timestamp_column].iat[0]
                end = self.df[self.timestamp_column].iat[-1]
        else:
            if use_timezone:
                start = min(self.df[self.timestamp_column]).tz_localize(tz='UTC')
                end = max(self.df[self.timestamp_column]).tz_localize(tz='UTC')
            else:
                start = min(self.df[self.timestamp_column])
                end = max(self.df[self.timestamp_column])

        return start, end

    def get_prior_timestamp(self, ts: pd.Timestamp) -> pd.Timestamp:
        """Get the first timestamp in the index that is before the given timestamp.

        This allows us to calibrate weekly/4 hours/etc. indexes to any given time..

        Example:

        .. code-block:: python

            raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()
            candle_universe = GroupedCandleUniverse(raw_candles)

            # Calibrate our week
            random_date = pd.Timestamp("2021-10-29")
            weekly_ts_before = candle_universe.get_prior_timestamp(random_date)

            assert weekly_ts_before == pd.Timestamp("2021-10-25")

        :return: Any timestamp from the index that is before or at the same time of the given timestamp.
        """
        index = self.df.index
        return index[index <= ts][-1]

    def get_single_pair_data(self,
                             timestamp: Optional[pd.Timestamp] = None,
                             sample_count: Optional[int] = None,
                             allow_current=False,
                             raise_on_not_enough_data=True,
                             time_range_epsilon_seconds=0.5,
                             ) -> pd.DataFrame:
        """Get all candles/liquidity samples for the single alone pair in the universe by a certain timestamp.

        A shortcut method for trading strategies that trade only one pair.
        Designed to be backtesting and live trading friendly function to access candle data.

        Example:

        .. code-block: python


                from tradingstrategy.utils.groupeduniverse import NoDataAvailable

                try:
                    candles: pd.DataFrame = universe.candles.get_single_pair_data(
                        timestamp,
                        sample_count=moving_average_long,
                    )
                except NoDataAvailable:
                    # This can be raised if
                    # - Data source has not yet data available in the timestamp
                    # - You are asking `sample_count` worth of data and timestamp
                    #   has not yet enough data in the backtest buffer
                    pass

        .. note ::

            By default get_single_pair_da   ta() returns the candles prior to the `timestamp`,
            the behavior can be changed with get_single_pair_data(allow_current=True).
            At the start of the backtest, we do not have any previous candle available yet,
            so this function may raise :py:class:`NoDataAvailable`.

        :param timestamp:
            Get the sample until this timestamp and all previous samples.

        :param allow_current:
            Allow to read any candle precisely at the timestamp.
            If you read the candle of your current strategy cycle timestamp,
            bad things may happen.

            In backtesting, reading the candle at the current timestamp
            introduces forward-looking bias. In live trading,
            reading the candle at the current timestamp may
            give you no candle or an incomplete candle (trades are still
            piling up on it).

        :param sample_count:
            Minimum candle/liquidity sample count needed.

            Limit the returned number of candles N candles before the timestamp.

            If the data does not have enough samples before `timestamp`,
            then raise :py:class:`NoDataAvailable`.

        :param raise_on_not_enough_data:
            Raise an error if no data is available.

            This can be e.g. because the trading pair has

        :param time_range_epsilon_seconds:
            The time delta epsilon we use to determine between "current" and "previous" candle.

        :raise NoDataAvailable:
            Raised when there is no data available at the range.

            Set `fail_on_empty=False` to return an empty `DataFrame` instead.

        """

        pair_count = self.get_pair_count()
        assert pair_count == 1, f"This function only works for single pair univese, we have {pair_count} pairs"
        df = self.df

        # Get all df content before our timestamp
        if timestamp:
            if allow_current:
                after = timestamp + pd.Timedelta(seconds=time_range_epsilon_seconds)
            else:
                after = timestamp - pd.Timedelta(seconds=time_range_epsilon_seconds)

            df = df.truncate(after=after)

        # Do candle count clip
        if sample_count:
            df = df.iloc[-sample_count:]
        else:
            pass

        # Be helpful with a possible error
        if raise_on_not_enough_data:
            if (sample_count is None and len(df) == 0) or (sample_count is not None and len(df) < sample_count):
                start_at = self.df["timestamp"].min()
                end_at = self.df["timestamp"].max()
                raise NoDataAvailable(f"Tried to ask for {sample_count} candles before timestamp {timestamp}, due to the sample count you specified of {sample_count}, but you only loaded candles from {start_at}. Truncating data after {after}. \n"\
                                      "\n\n"
                                      f"You have a few options in order of recommendation:\n"
                                      f"1. Try to increase the loaded data range when you create the universe, so that the backtest can start with past data ready.\n"
                                      f"2. Make the backtest start at least {sample_count} candles later\n"
                                      f"3. If you don't mind not having full past data for the first {sample_count} trade cycles, set `raise_on_not_enough_data=False in `get_single_pair_data`\n"
                                      f"\n\n"
                                      f"The result was {len(df)} candles. The trading pair or the time period does not have enough data.\n"
                                      f"The total loaded candle data is {len(self.df)} candles at range {start_at} - {end_at}.\n"
                                      f"\n"
                                      f"Make sure the strategy does not require data prior to {start_at}\n"
                                      f"\n"
                                      f"You cannot ask data for the current candle (same as the timestamp) unless you set allow_current=True.\n"
                                      f"\n"
                                      f"The current timestamp is ignored byt default protect against accidental testing of future data.\n"
                                      f"If you want to access empty or not enough data, set raise_on_not_enough_data=False.")

        return df

    def get_single_value(
        self,
        asset_id: PrimaryKey,
        when: pd.Timestamp | datetime.datetime,
        data_lag_tolerance: pd.Timedelta,
        kind="close",
        asset_name: str | None=None,
        link: str | None=None,
    ) -> Tuple[float, pd.Timedelta]:
        """Get a single value for a single pair/asset at a specific point of time.

        The data may be sparse data. There might not be sample available in the same time point or
        immediate previous time point. In this case the method looks back for the previous
        data point within `tolerance` time range.

        This method should be relative fast and optimised for any price, volume and liquidity queries.

        Example:

        .. code-block:: python

            # TODO

        :param asset_id:
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

        :param asset_name:
            Used in exception messages.

            If not given use ``asset_id``.

        :param link:
            Link to the asset page.

            Used in exception messages.

            If not given use ``<link unavailable>``.

        :return:
            Return (value, delay) tuple.

            We always return a value. In the error cases an exception is raised.
            The delay is the timedelta between the wanted timestamp
            and the actual timestamp of the sampled value.

            Candles are always timestamped by their opening.

        :raise NoDataAvailable:
            There were no samples available with the given condition.

        """

        assert kind in ("open", "close", "high", "low"), f"Got kind: {kind}"

        assert asset_id is not None, "asset_id must be given"

        if isinstance(when, datetime.datetime):
            when = pd.Timestamp(when)

        if not asset_name:
            asset_id = str(asset_name)

        if not link:
            link = "<link unavailable>"

        last_allowed_timestamp = when - data_lag_tolerance

        candles_per_pair = self.get_samples_by_pair(asset_id)

        if candles_per_pair is None:
            raise NoDataAvailable(
                f"No candle data available for asset {asset_name}, asset id {asset_id}\n"
                f"Trading data pair link: {link}")

        samples_per_kind = candles_per_pair[kind]

        # Fast path
        try:
            sample = samples_per_kind[when]
            return sample, pd.Timedelta(seconds=0)
        except KeyError:
            pass

        #
        # No direct hit. Either sparse data or data not available before this.
        # Lookup just got complicated,
        # like our relationship on Facebook.
        #

        # The indexes we can have are
        # - MultiIndex (pair id, timestamp) - if multiple trading pairs present
        # - DatetimeIndex - if single trading pair present

        if isinstance(candles_per_pair.index, pd.MultiIndex):
            timestamp_index = cast(pd.DatetimeIndex, candles_per_pair.index.get_level_values(1))
        elif isinstance(candles_per_pair.index, pd.DatetimeIndex):
            timestamp_index = candles_per_pair.index
        else:
            raise NotImplementedError(f"Does not know how to handle index {candles_per_pair.index}")

        # TODO: Do we need to cache the indexer... does it has its own storage?
        ffill_indexer = timestamp_index.get_indexer([when], method="ffill")

        before_match_iloc = ffill_indexer[0]

        if before_match_iloc < 0:
            # We get -1 if there are no timestamps where the forward fill could start
            first_sample_timestamp = timestamp_index[0]
            raise NoDataAvailable(
                f"Could not find any candles for pair {asset_name}, value kind '{kind}' at or before {when}\n"
                f"- Pair has {len(samples_per_kind)} samples\n"
                f"- First sample is at {first_sample_timestamp}\n"
                f"- Trading pair page link {link}\n"
            )
        before_match = timestamp_index[before_match_iloc]

        latest_or_equal_sample = candles_per_pair.iloc[before_match_iloc]

        # Check if the last sample before the cut off is within time range our tolerance
        candle_timestamp = before_match

        # Internal sanity check
        distance = when - candle_timestamp
        assert distance >= ZERO_TIMEDELTA, f"Somehow we managed to get a candle timestamp {candle_timestamp} that is newer than asked {when}"

        if candle_timestamp >= last_allowed_timestamp:
            # Return the chosen price column of the sample,
            # because we are within the tolerance
            return latest_or_equal_sample[kind], distance

        # We have data, but we are out of tolerance
        first_sample_timestamp = timestamp_index[0]
        last_sample_timestamp = timestamp_index[-1]

        raise NoDataAvailable(
            f"Could not find candle data for pair {asset_name}\n"
            f"- Column '{kind}'\n"
            f"- At {when}\n"
            f"- Lower bound of time range tolerance {last_allowed_timestamp}\n"
            f"\n"
            f"- Data lag tolerance is set to {data_lag_tolerance}\n"
            f"- The pair has {len(samples_per_kind)} candles between {first_sample_timestamp} - {last_sample_timestamp}\n"
            f"\n"
            f"Data unavailability might be due to several reasons:\n"
            f"\n"
            f"- You are handling sparse data - trades have not been made or the blockchain was halted during the price look-up period.\n"
            f"  Try to increase 'tolerance' argument time window.\n"
            f"- You are asking historical data when the trading pair was not yet live.\n"
            f"- Your backtest is using indicators that need more lookback buffer than you are giving to them.\n"
            f"  Try set your data load range earlier or your backtesting starting later.\n"
            f"\n",
            f"Trading pair page link: {link}"
            )

    def forward_fill(
        self,
        columns: Tuple[str] = ("open", "close"),
        drop_other_columns=True,
    ):
        """Forward-fill sparse OHLCV candle data.

        Forward fills the missing candle values for non-existing candles.
        Trading Strategy data does not have candles unless there was actual trades
        happening at the markets.

        See :py:mod:`tradingstrategy.utils.forward_fill` for details.

        .. note ::

            Does not touch the original `self.df` DataFrame any way.
            Only `self.pairs` is modified with forward-filled data.

        :param columns:
            Columns to fill.

            To save memory and speed, only fill the columns you need.
            Usually `open` and `close` are enough and also filled
            by default.

        :param drop_other_columns:
            Remove other columns before forward-fill to save memory.

            The resulting DataFrame will only have columns listed in `columns`
            parameter.

            The removed columns include ones like `high` and `low`, but also Trading Strategy specific
            columns like `start_block` and `end_block`. It's unlikely we are going to need
            forward-filled data in these columns.
        """

        self.pairs = forward_fill(
            self.pairs,
            self.time_bucket.to_frequency(),
            columns=columns,
            drop_other_columns=drop_other_columns,
        )

        # Clear candle cache
        self.clear_cache()


def filter_for_pairs(samples: pd.DataFrame, pairs: pd.DataFrame) -> pd.DataFrame:
    """Filter dataset so that it only contains data for the trading pairs from a certain exchange.

    Useful as a preprocess step for creating :py:class:`tradingstrategy.candle.GroupedCandleUniverse`
    or :py:class:`tradingstrategy.liquidity.GroupedLiquidityUniverse`.

    :param samples: Candles or liquidity dataframe

    :param pairs: Pandas dataframe with :py:class:`tradingstrategy.pair.DEXPair` content.
    """
    ids = pairs["pair_id"]
    our_pairs: pd.DataFrame = samples.loc[
        (samples['pair_id'].isin(ids))
    ]
    return our_pairs


def filter_for_single_pair(samples: pd.DataFrame, pair: DEXPair) -> pd.DataFrame:
    """Filter dataset so that it only contains data for a single trading pair.

    Useful to construct single trading pair universe.

    :param samples: Candles or liquidity dataframe
    """
    assert isinstance(pair, DEXPair), f"We got {pair}"
    our_pairs: pd.DataFrame = samples.loc[
        (samples['pair_id'] == pair.pair_id)
    ]
    return our_pairs


def resample_series(
    series: pd.Series,
    freq: pd.Timedelta,
    forward_fill: bool = False,
    backwards_fill: bool = False,
):
    """Downsample or upsample liquidity series. If upsamping, use forward_fill = True to fill in the missing values.
    
    Note, this does not apply to OHLCV candles, use :y:func:`resample_candle` and :py:func:`resample_price_series` for that.

    :param series: Series to resample

    :param freq: New timedelta to resample to

    :param forward_fill:
        Forward fill missing values if upsampling

    :param backwards_fill:
        Backwards fill.
    """
    
    series = series.astype(float)

    candles = series.resample(freq).mean(numeric_only=True)

    if forward_fill:
        candles = candles.fillna(method="ffill")

    if backwards_fill:
        candles = candles.fillna(method="bfill")

    return candles


def resample_candles(
    df: pd.DataFrame,
    resample_freq: pd.Timedelta | str,
    shift: int | None=None,
    origin: str | None=None,
) -> pd.DataFrame:
    """Downsample or upsample OHLCV candles or liquidity samples.

    E.g. upsample 1h candles to 1d candles.

    See also: py:func:`resample_price_series`.

    Example:

    .. code-block:: python

        # Transform daily candles to monthly candles
        from tradingstrategy.utils.groupeduniverse import resample_candles

        single_pair_candles = raw_candles.loc[raw_candles["pair_id"] == pair.pair_id]
        single_pair_candles = single_pair_candles.set_index("timestamp", drop=False)
        monthly_candles = resample_candles(single_pair_candles, TimeBucket.d30)
        monthly_candles = resample_candles(single_pair_candles, TimeBucket.d30)
        assert len(monthly_candles) <= len(single_pair_candles) / 4

    :param df:
        DataFrame of price, liquidity or lending rate candles.

        Must contain candles only for a single trading pair.

        Supported columns: open, high, low, close.
        Optional: pair_id, volume.

        Any other columns in DataFrame are destroyed in the resampling process.

    :param resample_freq:
        Resample frequency.

        Timedelta or Pandas alias string e.g. "D".

        E.g.`pd.Timedelta(days=1)` create daily candles from hourly candles.

    :param shift:
        Before resampling, shift candles to left or right.

        The shift is measured in number of candles, not time.
        Make sure the DataFrame is forward filled first,
        see :py:func:`forward_fill`.

        Set to `1` to shift candles one step right,
        `-1` to shift candles one step left.

        There might not be enough rows to shift. E.g. shift=-1 or shift=1 and len(df) == 1.
        In this case, an empty data frame is returned.

    :param origin:
        For daily resample, the starting hour.

        Use `origin="end"` for a rolling resample.

    :return:
        Resampled candles in a new DataFrame.

        Contains an added `timestamp` column that is also the index.

        If the input DataFrame is zero-length, then return it as is.

    """

    if not type(resample_freq) == str:
        assert isinstance(resample_freq, pd.Timedelta), f"We got {resample_freq}, supposed to be pd.Timedelta. E.g. pd.Timedelta(hours=2)"

    if len(df) == 0:
        return df

    # Sanity check we don't try to resample mixed data of multiple pairs
    if "pair_id" in df.columns:
        pair_ids = df["pair_id"].unique()
        assert len(pair_ids) == 1, f"Must have single pair_id only. We got {len(pair_ids)} pair ids: {pair_ids}, columns: {df.columns}"
        pair_id = pair_ids[0]
    else:
        pair_id = None

    ohlc_dict = {}

    if "open" in df.columns:
        ohlc_dict["open"] = "first"

    if "high" in df.columns:
        ohlc_dict["high"] = "max"

    if "low" in df.columns:
        ohlc_dict["low"] = "min"

    if "close" in df.columns:
        ohlc_dict["close"] = "last"

    if "volume" in df.columns:
        ohlc_dict["volume"] = "sum"

    columns = df.columns.tolist()
    assert all(item in columns for item in list(ohlc_dict.keys())), \
        f"{list(ohlc_dict.keys())} needs to be in the column names\n" \
        f"We got columns: {df.columns.tolist()}"

    if shift:
        df = df.shift(shift).dropna()

    if origin:
        candles = df.resample(resample_freq, origin=origin).agg(ohlc_dict)
    else:
        # https://stackoverflow.com/questions/21140630/resampling-trade-data-into-ohlcv-with-pandas
        candles = df.resample(resample_freq).agg(ohlc_dict)

    # TODO: Figure out right way to preserve timestamp column,
    # resample seems to destroy it
    candles["timestamp"] = candles.index

    if pair_id:
        candles["pair_id"] = pair_id

    return candles


def resample_dataframe(
    df: pd.DataFrame,
    resample_freq: pd.Timedelta,
    shift: int | None=None,
):
    """Resample a DataFrame to a lower frequency. Use this when your dataframe is not OHLCV candles.
    e.g. when using pandas_ta.bbands, your returned columns could be BBU_5_2.0, BBM_5_2.0, BBL_5_2.0, clearly not candlestick data

    :param df: DataFrame to resample
    :param resample_freq: Resample frequency
    :param shift: Shift the resampled data by a certain number of candles
    :return: Resampled DataFrame
    """

    def resample_wrapper(series: pd.Series):
        return resample_price_series(series, resample_freq, shift=shift)
    
    return df.apply(resample_wrapper)


def resample_rolling(df: pd.DataFrame, window:int = 24) -> pd.DataFrame:
    """Resample OHLCV feed using a rolling window technique.

    - OHLCV data is looked back from the current hour, not from the day boundary

    - Needed to avoid lookahead bias workarounds

    - Somewhat slow to compute, cache results on the disk

    - Note that the result might not be directly applicable to techical analysis functions:
      these might still expect you to downsaple the rolling window 1h data to daily,
      this function will make the data just available in a convinient way to downsample
      without lookahead bias

    See https://stackoverflow.com/q/78036231/315168

    :param df:
        DataFrame with columns "open", "high", "low", "close", "volume"

    :param window:
        How many samples per one OHLCV window.

        E.g. set to `24` to sample hourly to rolling daily.

    :return:
        DataFrame with a rolling OHCLV values as the candles form, without peeking to the future.

        There are no NA values. On the first row open = high = low = close because
        we sample over one value.

    """
    rolled = df.rolling(window=window, min_periods=1).agg(
        {
            "open": lambda x: x.iloc[0],  # First value in the window
            "high": "max",
            "low": "min",
            "close": lambda x: x.iloc[-1],  # Last value in the window
            "volume": lambda x: x.sum()
        }
    )
    return rolled


def resample_price_series(
    series: pd.Series,
    resample_freq: pd.Timedelta,
    shift: int | None=None,
    price_series_type="close"
) -> pd.Series:
    """Resample a price series to a lower frequency.

    See `test_price_series_resample_and_shift_binance` for some example output how shift works.
    Shift -1 means that any strategy decision is 1h delayed (close price is chosen 1h later).

    See also :py:func:`resample_candles`.

    TODO: Add forward-fill.

    :param series:
        Price series, e.g. close series.

        If the `series.index` is multi-index, assume it is (pair id, timestamp) for a single pair.

    :param resample_freq:
        Resample frequency.

        E.g.`pd.Timedelta(days=1)` create daily candles from hourly candles.

    :param shift:
        Before resampling, shift candles to left or right.

        The shift is measured in number of candles, not time.
        Make sure the DataFrame is forward filled first,
        see :py:func:`forward_fill`.

        Set to `1` to shift candles one step right,
        `-1` to shift candles one step left.

        There might not be enough rows to shift. E.g. shift=-1 or shift=1 and len(df) == 1.
        In this case, an empty data frame is returned.

    :param price_series_type:
        One of "open", "close", "high", "low"
    """
    assert isinstance(series, pd.Series), f"Expected pandas.Series, got {type(series)}"
    assert isinstance(resample_freq, pd.Timedelta), f"We got {resample_freq}, supposed to be pd.Timedelta. E.g. pd.Timedelta(hours=2)"
    if shift is not None:
        assert type(shift) == int

    # Deal with data that is in a grouped data frame
    if isinstance(series.index, pd.MultiIndex):
        # pair_id, timestamp tuples
        #MultiIndex([(2854973, '2021-12-21 19:00:00'),
        #            (2854973, '2021-12-21 20:00:00'),
        #            (2854973, '2021-12-21 21:00:00'),
        series = pd.Series(data=series.values, index=series.index.get_level_values(1))
        assert isinstance(series.index, pd.DatetimeIndex)

    match price_series_type:
        case "close":
            func = "last"
        case "open":
            func = "first"
        case "high":
            func = "high"
        case "low":
            func = "low"
        case _:
            raise NotImplementedError(f"Unknown price series type: {price_series_type}")

    if len(series) == 0:
        return series

    if shift:
        series = series.shift(shift).dropna()

    series = series.resample(resample_freq).agg(func)
    return series


def fix_bad_wicks(
    df: pd.DataFrame,
    threshold=(0.1, 1.9),
    too_slow_threshold=15,
    verbose=False,
    bad_open_close_threshold: float | None=3.0,
) -> pd.DataFrame:
    """Correct out bad high/low values in OHLC data.

    Applicable for both :term:`OHLCV` price feeds and liquidity feeds.

    On :term:`Uniswap` v2 and compatibles, Bad wicks are caused by e.g. very large flash loan, oracle price manipulation attacks,
    and misbheaving bots.

    This function removes bad high/low values and sets them to open/close if they seem to be wildly out of sample.

    :param threshold:
        How many pct % wicks are allowed through.

        Tuple (low threshold, high threshold) relative to close.

        Default to 50%. A high wick cannot be more than 50% of close.

    :param too_slow_threshold:
        Complain if this takes too long

    :param bad_open_close_threshold:
        How many X open must be above the high to be considered a broken data point.

        The open price will be replaced with high price.

        Do not set for liquidity processing.


    :param verbose:
        Make some debug logging when using the function for manual data diagnostics.

    """

    start = naive_utcnow()

    if len(df) == 0:
        return df


    # Optimised with np.where()
    # https://stackoverflow.com/a/65729035/315168
    if threshold is not None:
        df["high"] = np.where(df["high"] > df["close"] * threshold[1], df["close"], df["high"])
        df["low"] = np.where(df["low"] < df["close"] * threshold[0], df["close"], df["low"])

    # For manual diagnostics tracking down bad trading pair data
    if verbose and bad_open_close_threshold:
        bad_opens = df[df["open"] > df["high"] * bad_open_close_threshold]
        for idx, row in islice(bad_opens.iterrows(), 10):
            logger.warning(
                "Pair id %d, timestamp: %s, open: %s, high: %s, buy volume: %s sell volume: %s, volume: %s",
                row.pair_id,
                row.timestamp,
                row.open,
                row.high,
                row.get("buy_volume"),
                row.get("sell_volume"),
                row.get("volume"),
            )
        logger.warn("Total %d bad open price entries detected", len(bad_opens))

    # Issues in open price values with data point - open cannot be higher than high.
    # Not strickly "wicks" but we fix all data while we are at it.
    if bad_open_close_threshold:
        df["open"] = np.where(df["open"] > df["high"] * bad_open_close_threshold, df["high"], df["open"])
        df["close"] = np.where(df["close"] > df["high"] * bad_open_close_threshold, df["high"], df["close"])

    duration = naive_utcnow() - start

    if duration > datetime.timedelta(seconds=too_slow_threshold):
        logger.warning("Very slow fix_bad_wicks(): %s", duration)

    # The following code chokes
    # mask = (df["high"] > df["close"] * (1+threshold)) | (df["low"] < df["close"] * threshold)
    #df.loc[mask, "high"] = df["close"]
    #df.loc[mask, "low"] = df["close"]
    #df.loc[mask, "wick_filtered"] = True
    return df


def filter_bad_wicks(df: pd.DataFrame, threshold=(0.1, 1.9)) -> pd.DataFrame:
    """Mark the bad wicks.

    On :term:`Uniswap` v2 and compatibles, Bad wicks are caused by e.g. very large flash loan, oracle price manipulation attacks,
    and misbheaving bots.

    This function removes bad high/low values and sets them to open/close if they seem to be wildly out of sample.

    :param threshold:
        How many pct % wicks are allowed through as (low, high) tuple.

        This is a tuple (low threshold, high threshold).

        If low < close * threshold[0] ignore the value.

        If high > close * threshold[0] ignore the value.

    """

    df_matches = df.loc[
        (df["high"] > df["close"] * threshold[1]) | (df["low"] < df["close"] * threshold[0])
    ]

    return df_matches

def remove_zero_candles(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Remove any candle that has a zero value for OHLC
    
    :param df: Dataframe that may contain zero candles
    :return: pd.Dataframe
    """
    if len(df) > 0:
        filtered_df = df[(df['open'] != 0) & (df['high'] != 0) & (df['low'] != 0) & (df['close'] != 0)]
        return filtered_df
    return df