import logging
from typing import Optional, Tuple, Iterable, Dict

import pandas as pd

from tradingstrategy.types import PrimaryKey


logger = logging.getLogger(__name__)


class PairGroupedUniverse:
    """A base class for manipulating columnar sample data by a pair.

    The server dumps all pairs in the same continuous columnar dump
    For most the use cases, we want to manipulate data by pair.
    To achieve this, we use Pandas :py:class:`pd.GroupBy` and
    recompile the data on the client side.
    """

    def __init__(self, df: pd.DataFrame, timestamp_column="timestamp", index_automatically=True):
        """
        :param timestamp_column: What column use to build a time index
        :param index_automatically: Convert the index to use time series. You might avoid this with QSTrader kind of data.
        """
        assert isinstance(df, pd.DataFrame)
        if index_automatically:
            self.df = df.set_index(timestamp_column, drop=False)
        self.pairs: pd.GroupBy = self.df.groupby(["pair_id"])

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

    def get_all_samples_by_timestamp(self, ts: pd.Timestamp):
        """Get list of candles/samples for all pairs at a certain timepoint."""
        samples = self.df.loc[self.df["timestamp"] == ts]
        return samples
