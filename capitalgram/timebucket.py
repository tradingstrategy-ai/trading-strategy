"""Time window presentation."""

import enum


class TimeBucket(enum.Enum):
    """Supported time windows for candle and liquidity data.

    We use term "bucket", from the
    `TimescaleDB slang <https://docs.timescale.com/api/latest/continuous-aggregates/refresh_continuous_aggregate/>`_
    to symbol the time window of a candle data we are querying.

    The raw blockchain data is assembled to 1 minute time buckets.
    Then the 1 minute timebuckets are resampled to other windows.

    All time windows are in UTC.
    Daily time buckets have their hour, minute and second set to the zero in the outputted data.
    Hourly time buckets have minute and hour set to zero, etc.
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
    d1 = "d1"

    #: Weekly candles
    d7 = "7d"

    #: Monthly candles
    d30 = "30d"