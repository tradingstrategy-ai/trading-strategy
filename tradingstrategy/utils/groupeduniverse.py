"""Helpers to create Pandas dataframes for per-pair analytics."""

import logging
from typing import Optional, Tuple, Iterable

import pandas as pd

from tradingstrategy.pair import DEXPair
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.types import PrimaryKey
from tradingstrategy.utils.time import assert_compatible_timestamp


logger = logging.getLogger(__name__)


class PairGroupedUniverse:
    """A base class for manipulating columnar sample data by a pair.

    The server dumps all pairs in a single continuous data frame.
    For most the use cases, we want to look up and manipulate data by pairs.
    To achieve this, we use Pandas :py:class:`pd.GroupBy` and
    recompile the data on the client side.

    This works for

    - OHLCV candles

    - Liquidity candles

    The input :py:class:`pd.DataFrame` is sorted by default using `timestamp`
    column and then made this column as an index. This is not optimised (not inplace).
    """

    def __init__(self,
                 df: pd.DataFrame,
                 time_bucket=TimeBucket.d1,
                 timestamp_column="timestamp",
                 index_automatically=True):
        """
        :param time_bucket:
            What bar size candles we are operating at. Default to daily.
            TODO: Currently not used. Will be removed in the future versions.

        :param timestamp_column:
            What column use to build a time index. Used for QStrader / Backtrader compatibility.

        :param index_automatically:
            Convert the index to use time series. You might avoid this with QSTrader kind of data.
        """
        self.index_automatically = index_automatically
        assert isinstance(df, pd.DataFrame)
        if index_automatically:
            self.df = df \
                .set_index(timestamp_column, drop=False)\
                .sort_index(inplace=False)
            # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sort_index.html
        else:
            self.df = df
        self.pairs: pd.GroupBy = self.df.groupby(["pair_id"])
        self.timestamp_column = timestamp_column
        self.time_bucket = time_bucket

    def get_columns(self) -> pd.Index:
        """Get column names from the underlying pandas.GroupBy object"""
        return self.pairs.obj.columns

    def get_sample_count(self) -> int:
        """Return the dataset size - how many samples total for all pairs"""
        return len(self.df)

    def get_pair_count(self) -> int:
        """Return the number of pairs in this dataset"""
        return len(self.pairs.groups)

    def get_samples_by_pair(self, pair_id: PrimaryKey) -> Optional[pd.DataFrame]:
        """Get samples for a single pair.

        After the samples have been extracted, set `timestamp` as the index for the data.
        """
        try:
            pair = self.pairs.get_group(pair_id)
        except KeyError as e:
            raise KeyError(f"No OHLC samples for pair id {pair_id} in {self}") from e
        return pair

    def get_all_pairs(self) -> Iterable[Tuple[PrimaryKey, pd.DataFrame]]:
        """Go through all liquidity samples, one DataFrame per trading pair."""
        for pair_id, data in self.pairs:
            yield pair_id, data

    def get_pair_ids(self) -> Iterable[PrimaryKey]:
        """Get all pairs present in the dataset"""
        for pair_id, data in self.pairs:
            yield pair_id

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
        return samples.groupby("pair_id")

    def get_timestamp_range(self, use_timezone=False) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        """Return the time range of data we have for.

        :param use_timezone:
            The resulting timestamps will have their timezone set to UTC.
            If not set then naive timestamps are generated.

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
                             ) -> pd.DataFrame:
        """Get all candles/liquidity samples for the single alone pair in the universe by a certain timestamp.

        A shortcut method for trading strategies that trade only one pair.
        Designed to be backtesting and live trading friendly function to access candle data.

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
            Limit the returned number of candles N candles before the timestamp.
        """

        pair_count = self.get_pair_count()
        assert pair_count == 1, f"This function only works for single pair univese, we have {pair_count} pairs"
        df = self.df

        # Get all df content before our timestamp
        if timestamp:
            if allow_current:
                df = df.truncate(after=timestamp + pd.Timedelta(seconds=1))
            else:
                df = df.truncate(after=timestamp - pd.Timedelta(seconds=1))

        if sample_count:
            return df.iloc[-sample_count:]
        else:
            return df


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


def resample_candles(df: pd.DataFrame, new_bucket: TimeBucket) -> pd.DataFrame:
    """Downsample OHLCV candles or liquidity samples to less granular time bucket.

    E.g. transform 1h candles to 24h candles.

    Example:

    .. code-block:: python

        single_pair_candles = raw_candles.loc[raw_candles["pair_id"] == pair.pair_id]
        single_pair_candles = single_pair_candles.set_index("timestamp", drop=False)
        monthly_candles = upsample_candles(single_pair_candles, TimeBucket.d30)
        assert len(monthly_candles) <= len(single_pair_candles) / 4

    """
    pandas_time_delta = new_bucket.to_pandas_timedelta()
    # https://stackoverflow.com/questions/21140630/resampling-trade-data-into-ohlcv-with-pandas
    candles = df.resample(pandas_time_delta).mean(numeric_only=True)

    # TODO: Figure out right way to preserve timestamp column,
    # resample seems to destroy it
    candles["timestamp"] = candles.index

    return candles