"""Helpers to timestamp format and value conformity.

- We are operating on naive Python datetimes, all in UTC timezone

"""
import calendar
import datetime

import pandas as pd

#: Pre-instiated no difference pd.Tiemdelta for optimisation
ZERO_TIMEDELTA = pd.Timedelta(0)

def is_compatible_timestamp(ts: pd.Timestamp) -> bool:
    """Ensure Pandas Timestamp is naive.

    We do not carry timezone information in data, because it would slow
    us down.
    """
    assert isinstance(ts, pd.Timestamp), f"We assume pandas.Timestamp, but received {ts}"
    return ts.tz is None


def assert_compatible_timestamp(ts: pd.Timestamp):
    """Check we do not get in bad input timestamps.

    :raise: AssertionError if the timestamp object is incompatible
    """
    assert isinstance(ts, pd.Timestamp), f"not pd.Timestamp: {ts.__class__.__name__}={ts}"
    assert is_compatible_timestamp(ts), f"Received pandas.Timestamp in incompatible format: {type(ts)}: {ts}"


def to_int_unix_timestamp(dt: datetime.datetime) -> int:
    """Get datetime as UTC seconds since epoch."""
    # https://stackoverflow.com/a/5499906/315168
    return int(calendar.timegm(dt.utctimetuple()))

def to_iso(dt: datetime.datetime | None) -> str | None:
    """Convert naive UTC datetime to ISO format string.

    Useful as an encoder for dataclasses_json serialization.

    Example:

    .. code-block:: python

        @dataclass_json
        @dataclass
        class SomeEntity:
            updated_at: datetime.datetime | None = field(
                metadata=config(
                    encoder=to_iso,
                    decoder=from_iso,
                )
            )

    :param dt:
        Datetime to convert, or None

    :return:
        ISO format string, or None if input was None
    """
    return None if dt is None else dt.isoformat()


def from_iso(iso_str: str | None) -> datetime.datetime | None:
    """Parse ISO format string to naive UTC datetime.

    Useful as a decoder for dataclasses_json deserialization.
    See :py:func:`to_iso` for usage example.

    :param iso_str:
        ISO format string, or None

    :return:
        Naive UTC datetime, or None if input was None
    """
    return None if iso_str is None else datetime.datetime.fromisoformat(iso_str)


def generate_monthly_timestamps(start: datetime.datetime, end: datetime.datetime) -> list[int]:
    """Generate timestamps from the start to the end. One timestamp per month.

    :param start: Start date
    :param end: End date

    :return: List of timestamps
    """

    # TODO: ensure index has no missing dates i.e. evenly spaced intervals throughout the period
    timestamps = []
    current_date = start
    while current_date <= end:
        timestamps.append(int(current_date.timestamp()))
        # Check if adding one month stays within the year
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)

        if current_date > end:
            timestamps.append(int(end.timestamp()))
            break

    return timestamps


def naive_utcnow() -> datetime.datetime:
    """Get utcnow() but without timezone information.

    Fixes for Python 3.12 compatibility

    - https://blog.miguelgrinberg.com/post/it-s-time-for-a-change-datetime-utcnow-is-now-deprecated
    """
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def naive_utcfromtimestamp(timestamp: float | int) -> datetime.datetime:
    """Get naive UTC datetime from UNIX time.

    Fixes for Python 3.12 compatibility

    - https://blog.miguelgrinberg.com/post/it-s-time-for-a-change-datetime-utcnow-is-now-deprecated
    """
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc).replace(tzinfo=None)


def get_prior_timestamp(series: pd.Series, ts: pd.Timestamp) -> pd.Timestamp | None:
    """Get the first timestamp in the index that is before the given timestamp.

    :return:
        Any timestamp from the index that is before or at the same time of the given timestamp.

        Return ``None`` if there are no earlier timestamps.
    """

    index = series.index

    # The original data is in grouped DF
    if isinstance(index, pd.MultiIndex):
        # AssertionError: Got index: MultiIndex([(2854997, '2024-04-04 21:00:00'),
        #        (2854997, '2024-04-04 22:00:00'),
        index = index.get_level_values(1)

    assert isinstance(index, pd.DatetimeIndex), f"Got index: {index}"

    try:
        return index[index < ts][-1]
    except IndexError:
        return None


def floor_pandas_week(ts: pd.Timestamp) -> pd.Timestamp:
    """Round Pandas timestamp to a start of a week."""
    return ts.to_period("W").start_time


def floor_pandas_month(ts: pd.Timestamp) -> pd.Timestamp:
    """Round Pandas timestamp to a start of a month."""
    return ts.to_period("M").start_time


def to_unix_timestamp(dt: datetime.datetime) -> float:
    """Convert Python UTC datetime to UNIX seconds since epoch.

    Example:

    .. code-block:: python

        import datetime
        from eth_defi.utils import to_unix_timestamp

        dt = datetime.datetime(1970, 1, 1)
        unix_time = to_unix_timestamp(dt)
        assert unix_time == 0

    :param dt:
        Python datetime to convert

    :return:
        Datetime as seconds since 1970-1-1
    """
    # https://stackoverflow.com/a/5499906/315168
    return calendar.timegm(dt.utctimetuple())


def floor_month(dt: datetime.datetime) -> datetime.datetime:
    """Get first day of the month at 00:00:00

    :param dt:
        Python datetime to convert

    :return:
        Python datetime for first day of month at 00:00:00
    """
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def ceil_month(dt: datetime.datetime) -> datetime.datetime:
    """Get last day of the month at 23:59:59.999999

    :param dt:
        Python datetime to convert

    :return:
        Python datetime for last day of month at 23:59:59.999999
    """
    if dt.month == 12:
        next_month = dt.replace(year=dt.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        next_month = dt.replace(month=dt.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)

    return next_month - datetime.timedelta(microseconds=1)
