"""DataFrame index utilities.

- Deal with mismatch of having :term:`OHLCV` data is :py:class:`pd.DateTimeIndex` and :py:class:`pd.MultiIndex` format

"""
import pandas as pd


def _ensure_naive_datetime_index(index: pd.Index) -> pd.DatetimeIndex:
    """Convert any supported timestamp index to naive ``DatetimeIndex``.

    - Accepts ``DatetimeIndex``, Arrow-backed timestamp indexes, generic indexes,
      and pair/timestamp ``MultiIndex`` values
    - Keeps timestamps in UTC when timezone-aware values are encountered
    """

    if isinstance(index, pd.MultiIndex):
        assert len(index.levels) >= 2, f"Cannot infer timestamp level from index: {index}"
        index = index.get_level_values(-1)

    if isinstance(index, pd.DatetimeIndex):
        dt_index = index
    else:
        dt_index = pd.DatetimeIndex(pd.to_datetime(index))

    if dt_index.tz is not None:
        dt_index = dt_index.tz_convert("UTC").tz_localize(None)

    return dt_index


def flatten_dataframe_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure we have a datetime index as the index for the DataFrame.

    - Multipair data sources may have (pair_id, timestamp) index
      instead of (timestamp) index

    - Handle PyArrow-backed timestamp indexes that appear when loading
      from Parquet with newer pandas/pyarrow versions

    :return:
        DataFrame copy with a timestamp-only index
    """

    if isinstance(df.index, pd.DatetimeIndex):
        return df

    # PyArrow-backed timestamp columns produce a plain Index instead of DatetimeIndex
    # when used with set_index(). Convert to native DatetimeIndex.
    if not isinstance(df.index, pd.MultiIndex) and isinstance(df.index.dtype, pd.ArrowDtype):
        df2 = df.copy()
        df2.index = pd.DatetimeIndex(pd.to_datetime(df2.index))
        return df2

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


def normalise_timestamp_index(
    data: pd.DataFrame | pd.Series | pd.Index,
    timestamp_column: str | None = None,
    keep_timestamp_column: bool = False,
) -> pd.DataFrame | pd.Series | pd.DatetimeIndex:
    """Normalise timestamp-bearing data to use a naive ``DatetimeIndex``.

    This is a backwards-compatible helper retained for older callers in
    :mod:`tradeexecutor`.

    :param data:
        Data structure whose index should become a ``DatetimeIndex``.

    :param timestamp_column:
        Optional dataframe column to use as the timestamp source instead of
        the existing index.

    :param keep_timestamp_column:
        When ``timestamp_column`` is used, keep the source column instead of
        dropping it after reindexing.
    """

    if isinstance(data, pd.DataFrame):
        if timestamp_column is not None:
            assert timestamp_column in data.columns, f"DataFrame does not have timestamp column {timestamp_column}"
            df = data.copy()
            df.index = _ensure_naive_datetime_index(df[timestamp_column])
            if not keep_timestamp_column:
                df = df.drop(columns=[timestamp_column])
            return df

        df = flatten_dataframe_datetime_index(data)
        if not isinstance(df.index, pd.DatetimeIndex) or df.index.tz is not None:
            df = df.copy()
            df.index = _ensure_naive_datetime_index(df.index)
        return df

    if isinstance(data, pd.Series):
        if isinstance(data.index, pd.DatetimeIndex) and data.index.tz is None:
            return data

        series = data.copy()
        series.index = _ensure_naive_datetime_index(series.index)
        return series

    if isinstance(data, pd.Index):
        return _ensure_naive_datetime_index(data)

    raise TypeError(f"Unsupported data type for normalise_timestamp_index: {type(data)}")


def get_timestamp_index(df: pd.DataFrame) -> pd.DatetimeIndex:
    """Get DateTimeIndex for pair OHCLV data.

    See :py:func:`flatten_dataframe_datetime_index` for comments,

    - Return `df.index` or extract timestamp component from `MultiIndex`

    :return:
        DataFrame copy with a timestamp-only index
    """

    if isinstance(df.index, pd.DatetimeIndex):
        return df.index

    # PyArrow-backed timestamp indexes
    if not isinstance(df.index, pd.MultiIndex) and isinstance(df.index.dtype, pd.ArrowDtype):
        return pd.DatetimeIndex(pd.to_datetime(df.index))

    assert isinstance(df.index, pd.MultiIndex), f"Got wrong index: {type(df.index)}"

    # (pair id, timestamp) tuples
    assert pd.api.types.is_integer_dtype(df.index.levels[0])
    assert isinstance(df.index.levels[1], pd.DatetimeIndex)

    return df.index.get_level_values(1)
