import logging
from typing import Optional, Tuple, Iterable, Dict

import pandas as pd

from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.types import PrimaryKey


logger = logging.getLogger(__name__)


class PairGroupedUniverse:
    """A base class for manipulating columnar sample data by a pair.

    The server dumps all pairs in the same continuous columnar dump
    For most the use cases, we want to manipulate data by pair.
    To achieve this, we use Pandas :py:class:`pd.GroupBy` and
    recompile the data on the client side.
    """

    def __init__(self, df: pd.DataFrame, time_bucket=TimeBucket.d1, timestamp_column="timestamp", index_automatically=True):
        """

        :param time_bucket: What bar size candles we are operating at. Default to daily.

        :param timestamp_column: What column use to build a time index. Used for QStrader / Backtrader compatibility.

        :param index_automatically: Convert the index to use time series. You might avoid this with QSTrader kind of data.
        """
        assert isinstance(df, pd.DataFrame)
        if index_automatically:
            self.df = df.set_index(timestamp_column, drop=False)
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
        pair = self.pairs.get_group(pair_id)
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
        samples = self.df.loc[self.df[self.timestamp_column] == ts]
        return samples

    def get_timestamp_range(self) -> Tuple[pd.Timestamp, pd.Timestamp]:
        """Return the time range of data we have for.

        :return: (start timestamp, end timestamp) tuple
        """
        start = min(self.df[self.timestamp_column])
        end = max(self.df[self.timestamp_column])
        return start, end

