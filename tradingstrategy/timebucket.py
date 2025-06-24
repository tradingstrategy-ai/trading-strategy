"""Time window presentation."""
import datetime
import enum

import pandas as pd
from pandas.tseries.frequencies import to_offset

from tradingstrategy.utils.time import floor_pandas_week, floor_pandas_month


class NoMatchingBucket(Exception):
    """Cannot map timestamp to any available bucket."""


class TimeBucket(enum.Enum):
    """Supported time windows for :term:`candle` and :term:`liquidity` data.

    We use term "bucket", from the
    `TimescaleDB slang <https://docs.timescale.com/api/latest/continuous-aggregates/refresh_continuous_aggregate/>`_
    to symbol the time window of a candle data we are querying.

    The raw blockchain data is assembled to 1 minute time buckets.
    Then the 1 minute timebuckets are resampled to other windows.

    All time windows are in UTC.
    Daily time buckets have their hour, minute and second set to the zero in the outputted data.
    Hourly time buckets have minute and hour set to zero, etc.

    Python labels are reserved from the actual values, because Python symbol cannot start with a number.
    """

    #: One minute candles
    m1 = "1m"

    #: Five minute candles
    m5 = "5m"

    #: Quarter candles
    m15 = "15m"

    #: Hourly candles
    h1 = "1h"

    #: Four hour candles
    h4 = "4h"

    #: Eight  hour candles
    h8 = "8h"

    #: Daily candles
    d1 = "1d"

    #: Weekly candles
    d7 = "7d"

    #: Monthly candles
    d30 = "30d"

    #: We do not have "yearly" candles, but some trade statistics are calculated
    #: for 360 days, thus we need a corresponding time bucket for them.
    #:
    #: TOOD: Use `d365` instead.
    #:
    d360 = "360d"

    #: Yearly candles, using standard 365d year
    d365 = "365d"

    #: Some statistics like "all time high", for example, only make sense if a "bucket"
    #: spans across the entire timeline.
    infinite = "infinite"

    #: A placeholder value representing a "NULL value" for cases where Python's None
    #: is not a favorable choice for some reason.
    not_applicable = "not_applicable"

    def to_hours(self) -> float:
        """The length of this bucket as hours."""
        return self.to_timedelta() / datetime.timedelta(hours=1)

    def to_timedelta(self) -> datetime.timedelta:
        """Get delta object for a TimeBucket definition.

        You can use this to construct arbitrary timespans or iterate candle data.
        """
        return _DELTAS[self]

    def to_pandas_timedelta(self) -> pd.Timedelta:
        """Get pandas delta object for a TimeBucket definition.

        You can use this to construct aregime-filter.ipynbrbitrary timespans or iterate candle data, or to compare with two timebuckets.
        """
        return pd.Timedelta(_DELTAS[self])

    def to_frequency(self) -> pd.DateOffset:
        """Get frequency input for Pandas fuctions.

        You can use this to construct arbitrary timespans or iterate candle data.
        """
        if self in {TimeBucket.infinite, TimeBucket.not_applicable}:
            raise ValueError(f"Enum member {self} cannot be mapped to a frequency.")

        delta = self.to_timedelta()
        return to_offset(delta)

    def floor(self, timestamp: pd.Timestamp) -> pd.Timestamp:
        """Floor the time bucket to the nearest value.

        - Handle business week as d7
        """
        if self == TimeBucket.d7:
            # Floor down to the business week start
            return floor_pandas_week(timestamp)
        elif self == TimeBucket.d30:
            return floor_pandas_month(timestamp)

        return timestamp.floor(self.to_frequency())

    def floor_datetime(self, timestamp: datetime.datetime) -> datetime:
        """Floor the time bucket to the nearest value.

        - See :py:meth:`floor` for details.
        """
        return self.floor(pd.Timestamp(timestamp)).to_pydatetime()

    def ceil(self, timestamp: pd.Timestamp) -> pd.Timestamp:
        """Round up the time bucket to the nearest value.

        - Handle business week as d7
        """
        if self == TimeBucket.d7:
            # Floor down to the business week start
            return floor_pandas_week(timestamp)
        elif self == TimeBucket.d30:
            return floor_pandas_month(timestamp)

        return timestamp.ceil(self.to_frequency())


    @staticmethod
    def from_pandas_timedelta(td: pd.Timedelta) -> "TimeBucket":
        """Map Pandas timedelta to a well-known time bucket enum.

        :raise NoMatchingBucket:
            Could not map to any well known time bucket.
        """
        assert isinstance(td, pd.Timedelta)
        python_dt = td.to_pytimedelta()
        for k, v in _DELTAS.items():
            if python_dt == v:
                return k
        raise NoMatchingBucket(f"Could not map: {td}")
    
    def __lt__(self, other: "TimeBucket") -> bool:
        """Compare two time buckets."""
        return self.to_timedelta() < other.to_timedelta()

    def __le__(self, other: "TimeBucket") -> bool:
        """Compare two time buckets."""
        return self.to_timedelta() <= other.to_timedelta()

    def __gt__(self, other: "TimeBucket") -> bool:
        """Compare two time buckets."""
        return self.to_timedelta() > other.to_timedelta()

    def __ge__(self, other: "TimeBucket") -> bool:
        """Compare two time buckets."""
        return self.to_timedelta() >= other.to_timedelta()


# datetime.timedelta equivalents of different time buckets
_DELTAS = {
    TimeBucket.m1: datetime.timedelta(minutes=1),
    TimeBucket.m5: datetime.timedelta(minutes=5),
    TimeBucket.m15: datetime.timedelta(minutes=15),
    TimeBucket.h1: datetime.timedelta(hours=1),
    TimeBucket.h4: datetime.timedelta(hours=4),
    TimeBucket.h8: datetime.timedelta(hours=8),
    TimeBucket.d1: datetime.timedelta(days=1),
    TimeBucket.d7: datetime.timedelta(days=7),
    TimeBucket.d30: datetime.timedelta(days=30),
    TimeBucket.d360: datetime.timedelta(days=360),
    TimeBucket.infinite: datetime.timedelta.max,
    TimeBucket.not_applicable: datetime.timedelta(0),  # some sort of a NULL value
}


