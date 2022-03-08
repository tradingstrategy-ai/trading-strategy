"""Helpers to ensure timestamp data stays clean."""

import pandas as pd


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
    assert is_compatible_timestamp(ts), f"Received pandas.Timestamp in incompatible format: {ts}"
