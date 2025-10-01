""""""

from collections import defaultdict
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from datetime import datetime
import logging
import os
from typing import Collection, NamedTuple

from tradingstrategy.types import PrimaryKey
from tradingstrategy.utils.time import naive_utcfromtimestamp, to_unix_timestamp


logger = logging.getLogger(__name__)


def encode_datetime(dt: datetime | None) -> float | None:
    """Convert datetime to timestamp, handling None"""
    return to_unix_timestamp(dt) if dt is not None else None

def decode_datetime(ts: float | None) -> datetime | None:
    """Convert timestamp to datetime, handling None"""
    return naive_utcfromtimestamp(ts) if ts is not None else None


@dataclass_json
@dataclass(slots=True)
class PairCandleInfo:
    """Single pair candle info item (see PairCandleMetadata class below)"""
    start_time: datetime | None = field(
        default=None,
        metadata=config(  # type: ignore
            encoder=encode_datetime,
            decoder=decode_datetime
        )
    )

    end_time: datetime | None = field(
        default=None,
        metadata=config(  # type: ignore
            encoder=encode_datetime,
            decoder=decode_datetime
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
            Tuple of (full_fetch_ids, delta_fetch_ids)
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
