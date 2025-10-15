"""Pair candle caching utilities."""

from collections import defaultdict
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from datetime import datetime, timedelta
import logging
import os
import pathlib
from typing import Collection, NamedTuple

import pandas as pd

from tradingstrategy.transport.cache_utils import wait_other_writers
from tradingstrategy.types import PrimaryKey
from tradingstrategy.utils.time import naive_utcfromtimestamp, from_iso, to_iso


logger = logging.getLogger(__name__)

DEFAULT_CANDLE_LOOKBACK_HOURS = 48


@dataclass_json
@dataclass(slots=True)
class PairCandleInfo:
    """Single pair candle info item

    See :py:class:`PairCandleMetadata`
    """
    start_time: datetime | None = field(
        default=None,
        metadata=config(
            encoder=to_iso, # type: ignore
            decoder=from_iso
        )
    )

    end_time: datetime | None = field(
        default=None,
        metadata=config(
            encoder=to_iso, # type: ignore
            decoder=from_iso
        )
    )

    def update(self, start_time: datetime, end_time: datetime) -> None:
        """Update time range, expanding to include new times

        :param start_time:
            Start time that was used to fetch new candles.

        :param end_time:
            End time that was used to fetch new candles.
        """
        if self.start_time is None:
            self.start_time = start_time
        else:
            self.start_time = min(self.start_time, start_time)

        if self.end_time is None:
            self.end_time = end_time
        else:
            self.end_time = max(self.end_time, end_time)


class PairFetchPartition(NamedTuple):
    """Return type for :py:meth:`PairCandleMetadata.partition_for_fetch`"""
    full_fetch_ids: set[PrimaryKey]
    delta_fetch_ids: set[PrimaryKey]


@dataclass_json
@dataclass(slots=True)
class PairCandleMetadata:
    """Utility class for serialising / deserialising pair candle metadata

    The metadata file stores a map of pair_ids, with start_time and end_time
    per pair_id. These values represent the start and end dates of candles that
    have been fetched.

    The metadata dates may be different than the earliest or latest candle
    entries for the corresponding pair - e.g. if the pair's trading activity
    began at a date later than the earliest requested start date.
    """
    pairs: dict[str, PairCandleInfo] = field(default_factory=dict) # type: ignore

    def __post_init__(self):
        """Convert pairs dict to defaultdict for auto-creation"""
        self.pairs = defaultdict(lambda: PairCandleInfo(), self.pairs)

    # Non-serialized instance variables
    _file_path: str = field(default="", repr=False, metadata=config(exclude=lambda x: True))
    _last_modified_at: datetime | None = field(default=None, repr=False, metadata=config(exclude=lambda x: True))

    @classmethod
    def load(cls, fname: str) -> "PairCandleMetadata":
      """Load metadata from file, or create empty if doesn't exist

      :param fname:
          File location where metadata will be loaded from (if present) and saved to
      """
      try:
          with open(fname, "r") as f:
              metadata = cls.from_json(f.read())
              metadata._last_modified_at = naive_utcfromtimestamp(os.path.getmtime(fname))
              logger.debug(f"Using candles metadata file {fname}")
      except FileNotFoundError:
          # Create empty metadata
          logger.debug(f"No metadata file found: {fname}; initializing new metadata")
          metadata = cls(pairs={})

      metadata._file_path = fname
      return metadata

    def latest_end_time(self) -> datetime | None:
        """Latest end_time value across all pairs (or None)"""
        end_times = [pair.end_time for pair in self.pairs.values() if pair.end_time is not None]
        return max(end_times) if end_times else None

    @property
    def last_modified_at(self) -> datetime | None:
        """Get the modification time when this metadata was loaded, or None if newly created"""
        return self._last_modified_at

    def save(self) -> None:
        """Save metadata to the file it was loaded from"""
        if not self._file_path:
            raise ValueError("Cannot save: no file path set")

        with open(self._file_path, "w") as f:
            f.write(self.to_json(indent=2))

        # Update last_modified_at after save
        self._last_modified_at = datetime.fromtimestamp(os.path.getmtime(self._file_path))

        logger.debug(f"Write {self._file_path}, {len(self.pairs):,} pairs")

    def update(
        self,
        pair_ids: Collection[PrimaryKey],
        start_time: datetime,
        end_time: datetime
    ) -> None:
        """Update pair entries with new start_time and end_time

        If the existing entry has an earlier start or later end, the furthest extent
        values are retained.

        :param pair_ids:
            Trading pairs internal ids that are included in the pair candle cache.

        :param start_time:
            The new start_time that candles were fetched with.

        :param end_time:
            The new end_time that candles were fetched with.
        """
        for pair_id in pair_ids:
            self.pairs[str(pair_id)].update(start_time, end_time)

        logger.debug(f"Updated {len(pair_ids):,} pairs of {len(self.pairs):,} total")

    def partition_for_fetch(
        self,
        pair_ids: Collection[PrimaryKey],
        start_time: datetime,
        end_time: datetime
    ) -> PairFetchPartition:
        """Partition pair IDs into full fetch vs. delta fetch requirements.

        The partition is determined based on the existing cache metadata and
        the requested start and end times.

        :param pair_ids:
            Trading pairs internal ids to be partitioned (may or may not be in cache).

        :param start_time:
            The new start_time that candles will be fetched with.

        :param end_time:
            The new end_time that candles will be fetched with.

        :return:
            :py:class:`PairFetchPartition` - named tuple of `full_fetch_ids`, `delta_fetch_ids`
        """
        latest_end_time = self.latest_end_time()

        full_fetch_ids: set[PrimaryKey] = set()
        delta_fetch_ids: set[PrimaryKey] = set()

        for pair_id in pair_ids:
            pair_info = self.pairs[(str(pair_id))]

            if pair_info.start_time is None or pair_info.end_time is None:
                # New pair - need full history
                full_fetch_ids.add(pair_id)
            elif start_time < pair_info.start_time:
                # Need data before what we have cached - full fetch required
                full_fetch_ids.add(pair_id)
            elif end_time <= pair_info.end_time:
                # All requested data already cached - no fetch needed
                pass  # Add to neither list
            elif latest_end_time > pair_info.end_time: # type: ignore
                # Gap between cached data for this pair and latest_end_time - need full fetch
                full_fetch_ids.add(pair_id)
            else:
                # Only need recent delta from latest_end_time to end_time
                delta_fetch_ids.add(pair_id)

        logger.info(f"Pair candle fetch partition: full: {len(full_fetch_ids)}, delta: {len(delta_fetch_ids)}")

        return PairFetchPartition(
            full_fetch_ids=full_fetch_ids,
            delta_fetch_ids=delta_fetch_ids
        )

    def delta_fetch_start_time(self, lookback_hours: int = DEFAULT_CANDLE_LOOKBACK_HOURS) -> datetime | None:
        """Calculate the start time for delta fetches, accounting for data freshness.

        Returns the earlier of:
        - last_modified_at minus lookback window (to refetch potentially incomplete data)
        - latest_end_time (to avoid gaps)

        Returns None if metadata has never been saved.
        """
        if self.last_modified_at is None:
            return None

        freshness_cutoff = self.last_modified_at - timedelta(hours=lookback_hours)
        latest_end_time = self.latest_end_time()

        if latest_end_time is None:
            return freshness_cutoff

        return min(freshness_cutoff, latest_end_time)


class PairCandleCache:
    """Context manager for pair candle cache operations.

    Handles loading, updating, and saving of cached candle data and metadata.
    Designed to be used as a context manager to ensure proper file locking.

    Example usage:
        with PairCandleCache(cache_path) as cache:
            partition = cache.metadata.partition_for_fetch(pair_ids, start_time, end_time)
            # ... perform fetches ...
            cache.update([df1, df2])  # Update cache with new data
            return cache.data[] # filter as needed
    """

    def __init__(self, base_path: str):
        """Initialize cache with base file path.

        :param base_path:
            Absolute path without extension (e.g., "/path/to/candles-1h").
            Extensions .parquet, .json, .lock will be appended as needed.
        """
        self.base_path = base_path
        self.parquet_path = f"{base_path}.parquet"
        self.metadata_path = f"{base_path}.json"

        self._lock_context = None
        self._data = None
        self._metadata = None

    def __enter__(self) -> "PairCandleCache":
        """Enter context manager, acquiring file lock."""
        self._lock_context = wait_other_writers(self.base_path)
        self._lock_context.__enter__()

        # Load existing data and metadata
        self._load_data()
        self._load_metadata()

        return self


    def __exit__(self, *args: object):
        """Exit context manager, releasing file lock."""
        if self._lock_context:
            self._lock_context.__exit__(*args)  # type: ignore

    def _load_data(self) -> None:
        """Load existing parquet data if available.

        Restores "timestamp" index after loading.
        """
        if os.path.exists(self.parquet_path):
            try:
                logger.debug(f"Using cached candles file {self.parquet_path}")
                self._data = pd.read_parquet(self.parquet_path).set_index("timestamp", drop=False)
            except Exception as e:
                logger.warning(f"Failed to load cached parquet file: {e}. Using empty DataFrame instead.")
                self._data = pd.DataFrame()
        else:
            logger.debug(f"No cached candles file found: {self.parquet_path}. Using empty DataFrame instead.")
            self._data = pd.DataFrame()

    def _save_data(self) -> None:
        """Save the dataframe to parquet.

        The index is not saved for performance reasons.
        See :py:meth:`_load_data` for index restoration.
        """
        self._data.to_parquet(self.parquet_path, index=False) # type: ignore
        size = pathlib.Path(self.parquet_path).stat().st_size
        logger.debug(f"Wrote {os.path.basename(self.parquet_path)}, disk size is {size:,}b")

    def _load_metadata(self) -> None:
        """Load metadata from JSON file."""
        self._metadata = PairCandleMetadata.load(self.metadata_path)

    @property
    def data(self) -> pd.DataFrame:
        """Access to the cached candle DataFrame."""
        assert self._data is not None, "Cache not properly initialized - use as context manager"
        return self._data

    @property
    def metadata(self) -> PairCandleMetadata:
        """Access to the cache metadata."""
        assert self._metadata is not None, "Cache not properly initialized - use as context manager"
        return self._metadata

    def update(
        self,
        new_dataframes: list[pd.DataFrame],
        pair_ids: Collection[PrimaryKey],
        start_time: datetime,
        end_time: datetime
    ) -> None:
        """Update cache with new candle data.

        Concatenates new data with existing, removes duplicates, sorts, and saves
        both the parquet file and metadata.

        :param new_dataframes:
            List of DataFrames containing new candle data to add to cache.

        :param pair_ids:
            Trading pairs that were included in the fetch operation.

        :param start_time:
            Start time used for the fetch operation.

        :param end_time:
            End time used for the fetch operation.
        """
        assert self._data is not None, "Data should be initialized"

        # Only update parquet data if there are new dataframes to add
        if new_dataframes:
            # Append updated candles, remove duplicates, sort, reset index
            self._data = (
                pd.concat([self._data, *new_dataframes], ignore_index=True)
                  .drop_duplicates(subset=["pair_id", "timestamp"], keep="last")
                  .sort_values(["pair_id", "timestamp"])  # type: ignore
                  .set_index("timestamp", drop=False)
            )

            self._save_data()

        # Always update and save metadata for tracking
        self.metadata.update(pair_ids, start_time, end_time)
        self.metadata.save()
