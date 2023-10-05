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


