"""Detect gaps and bugs in timeseries data, and deal with it."""
from dataclasses import dataclass

import pandas as pd
import numpy as np


@dataclass(frozen=True, slots=True)
class Gap:
    # Gap size in entries
    gap_size: int



def detect_frequency(series: pd.Series) -> str:
    """Automatically detect the frequency of a time series.

    Parameters:
    -----------
    series : pandas.Series
        Input time series with DateTimeIndex

    Returns:
    --------
    str
        Detected frequency as a string (e.g., 'D', 'H', 'T', etc.)
    """
    if not isinstance(series.index, pd.DatetimeIndex):
        raise ValueError("Series must have a DateTimeIndex")

    if len(series.index) < 2:
        raise ValueError("Series must have at least 2 timestamps to detect frequency")

    # Calculate all time differences between consecutive timestamps
    time_diffs = np.diff(series.index)

    # Get the most common time difference
    most_common_diff = pd.Timedelta(np.median(time_diffs))

    # Convert timedelta to frequency string
    seconds = most_common_diff.total_seconds()

    if seconds < 1:
        milliseconds = most_common_diff.total_seconds() * 1000
        return f'{int(milliseconds)}L'  # milliseconds
    elif seconds < 60:
        return f'{int(seconds)}S'  # seconds
    elif seconds < 3600:
        minutes = seconds / 60
        if minutes.is_integer():
            return f'{int(minutes)}T'  # minutes
    elif seconds < 86400:
        hours = seconds / 3600
        if hours.is_integer():
            return f'{int(hours)}H'  # hours
    elif seconds < 604800:
        days = seconds / 86400
        if days.is_integer():
            return 'D'  # days
    elif seconds < 2592000:
        weeks = seconds / 604800
        if weeks.is_integer():
            return 'W'  # weeks
    elif seconds < 31536000:
        months = seconds / 2592000
        if months.is_integer():
            return 'M'  # months
    else:
        years = seconds / 31536000
        if years.is_integer():
            return 'Y'  # years

    # If no standard frequency matches, return in seconds
    return f'{int(seconds)}S'


def detect_timestamp_gaps(series, freq=None) -> list[Gap]:
    """
    Detect and print gaps in a time series.

    Parameters:
    -----------
    series : pandas.Series
        Input time series with DateTimeIndex
    freq : str, optional
        Frequency to use for gap detection. If None, will automatically detect frequency.
        Common options: 'D' for daily, 'H' for hourly, 'T' or 'min' for minute,
        'S' for second

    Returns:
    --------
    list of tuples
        List of (gap_start, gap_end, gap_size) tuples representing the gaps
    """
    # Ensure we have a DateTimeIndex
    if not isinstance(series.index, pd.DatetimeIndex):
        raise ValueError("Series must have a DateTimeIndex")

    # If frequency not provided, detect it
    if freq is None:
        freq = detect_frequency(series)

    # Create a complete date range
    full_index = pd.date_range(
        start=series.index.min(),
        end=series.index.max(),
        freq=freq
    )

    # Find missing dates
    missing_dates = full_index.difference(series.index)

    # If no gaps found, return empty list
    if len(missing_dates) == 0:
        return []

    # Find consecutive gaps
    gaps = []
    gap_start = missing_dates[0]
    prev_date = missing_dates[0]

    for date in missing_dates[1:]:
        expected_next = prev_date + pd.Timedelta(freq)
        if date != expected_next:
            # Gap ends, store it and start new gap
            gap_size = len(pd.date_range(gap_start, prev_date, freq=freq))
            gaps.append((gap_start, prev_date, gap_size))
            gap_start = date
        prev_date = date

    # Add the last gap
    gap_size = len(pd.date_range(gap_start, prev_date, freq=freq))
    gaps.append((gap_start, prev_date, gap_size))

    return gaps



def fill_missing_ohlcv(df, columns_to_fill=['open', 'high', 'low', 'close', 'volume', 'tvl']):
    """
    Fill missing timestamps for each pair_id with zeros for specified columns.

    Parameters:
    -----------
    df : pandas.DataFrame
        Input DataFrame with MultiIndex (pair_id, timestamp)
    columns_to_zero : list, optional
        Columns to fill with zeros when data is missing

    Returns:
    --------
    pandas.DataFrame
        DataFrame with missing timestamps filled with zeros
    """
    # Get full timestamp range across all pair_ids
    full_timestamp_range = df.index.get_level_values('timestamp').unique()

    # Create a new index with all combinations of pair_ids and timestamps
    pair_ids = df.index.get_level_values('pair_id').unique()
    multi_index = pd.MultiIndex.from_product([pair_ids, full_timestamp_range],
                                             names=['pair_id', 'timestamp'])

    # Reindex the original DataFrame
    filled_df = df.reindex(multi_index)

    # Fill specified columns with zeros where data is missing
    # for col in columns_to_zero:
    #    filled_df[col] = filled_df[col]

    return filled_df


def equalise_timestamp_index(
    data: pd.Series,
):
    """Make all pair data series equally length.

    :param series:
        pandas.Series of (pair_id, timestamp) multiindex
    """
    assert isinstance(data.index, pd.MultiIndex)

    unique_pair_ids = data.index.unique(level='pair_id')
    unique_timestamps = data.index.unique(level='timestamp')

    # Create a full MultiIndex with all combinations
    full_index = pd.MultiIndex.from_product(
        [unique_pair_ids, unique_timestamps],
        names=['pair_id', 'timestamp']
    )

    # Reindex the Series to fill missing values with NaN
    filled_series = data.reindex(full_index)

    return filled_series
