"""DataFrame index utilities.

- Deal with mismatch of having :term:`OHLCV` data is :py:class:`pd.DateTimeIndex` and :py:class:`pd.MultiIndex` format

"""
import pandas as pd


def flatten_dataframe_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure we have a datetime index as the index for the DataFrame.

    - Multipair data sources may have (pair_id, timestamp) index
      instead of (timestamp) index

    :return:
        DataFrame copy with a timestamp-only index
    """

    if isinstance(df.index, pd.DatetimeIndex):
        return df

    assert isinstance(df.index, pd.MultiIndex), f"Got wrong index: {type(df.index)}"

    # (pair id, timestamp) tuples
    # For some reason, sometimes pair_id comes out as float64?
    assert pd.api.types.is_integer_dtype(df.index.levels[0]) or pd.api.types.is_float_dtype(df.index.levels[0]), f"df.index.level[0] is {df.index.levels[0]} {type(df.index.levels[0])}"
    assert isinstance(df.index.levels[1], pd.DatetimeIndex), f"Got: {df.index.levels[1]} {type(df.index.levels[1])}"

    new_index = df.index.get_level_values(1)  # assume pair id, timestamp tuples
    assert isinstance(new_index, pd.DatetimeIndex), f"Got index: {type(new_index)}"
    df2 = df.copy()
    df2.index = new_index
    return df2


def get_timestamp_index(df: pd.DataFrame) -> pd.DatetimeIndex:
    """Get DateTimeIndex for pair OHCLV data.
    
    See :py:func:`flatten_dataframe_datetime_index` for comments,

    - Return `df.index` or extract timestamp component from `MultiIndex`

    :return:
        DataFrame copy with a timestamp-only index
    """

    if isinstance(df.index, pd.DatetimeIndex):
        return df.index

    assert isinstance(df.index, pd.MultiIndex), f"Got wrong index: {type(df.index)}"

    # (pair id, timestamp) tuples
    assert pd.api.types.is_integer_dtype(df.index.levels[0])
    assert isinstance(df.index.levels[1], pd.DatetimeIndex)

    return df.index.get_level_values(1)
