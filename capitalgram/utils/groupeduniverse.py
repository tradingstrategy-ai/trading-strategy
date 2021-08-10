from typing import Optional, Tuple, Iterable

import pandas as pd

from capitalgram.types import PrimaryKey


class PairGroupedUniverse:
    """A base class for manipulating columnar sample data by a pair.

    The server dumps all pairs in the same continuous columnar dump
    For most the use cases, we want to manipulate data by pair.
    To achieve this, we use Pandas :py:class:`pd.GroupBy` and
    recompile the data on the client side.
    """

    def __init__(self, df: pd.DataFrame):
        assert isinstance(df, pd.DataFrame)
        self.df = df
        self.pairs: pd.GroupBy = df.groupby(["pair_id"])

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
        if pair is not None:
            pair = pair.set_index(pair["timestamp"])
            return pair
        return None

    def get_all_pairs(self) -> Iterable[Tuple[PrimaryKey, pd.DataFrame]]:
        """Go through all liquidity samples, one DataFrame per trading pair."""
        for pair_id, data in self.pairs:
            yield pair_id, data

    def get_pair_ids(self) -> Iterable[PrimaryKey]:
        """Get all pairs present in the dataset"""
        for pair_id, data in self.pairs:
            yield pair_id
