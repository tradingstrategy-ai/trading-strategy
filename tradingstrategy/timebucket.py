"""Time window presentation."""
import datetime
import enum

import pandas as pd
from pandas.tseries.frequencies import to_offset


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

    #: Daily candles
    d1 = "1d"

    #: Weekly candles
    d7 = "7d"

    #: Monthly candles
    d30 = "30d"

    def to_timedelta(self) -> datetime.timedelta:
        """Get delta object for a TimeBucket definition.

        You can use this to construct arbitrary timespans or iterate candle data.
        """
        return _DELTAS[self]

    def to_frequency(self) -> pd.DateOffset:
        """Get frequncy input for Pandas fuctions.

        You can use this to construct arbitrary timespans or iterate candle data.
        """
        delta = self.to_timedelta()
        return to_offset(delta)


# datetime.timedelta equivalents of different time buckets
_DELTAS = {
    TimeBucket.m1: datetime.timedelta(minutes=1),
    TimeBucket.m5: datetime.timedelta(minutes=5),
    TimeBucket.m15: datetime.timedelta(minutes=15),
    TimeBucket.h1: datetime.timedelta(hours=1),
    TimeBucket.h4: datetime.timedelta(hours=4),
    TimeBucket.d1: datetime.timedelta(days=1),
    TimeBucket.d7: datetime.timedelta(days=7),
    TimeBucket.d30: datetime.timedelta(days=30),
}


