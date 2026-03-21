"""DataFrame index utilities.

- Deal with mismatch of having :term:`OHLCV` data is :py:class:`pd.DateTimeIndex` and :py:class:`pd.MultiIndex` format

"""
import pandas as pd


def normalise_timestamp_index(
    data: pd.Index | pd.Series | pd.DataFrame,
    timestamp_column: str | None = None,
    timestamp_level: int | str = "timestamp",
    keep_timestamp_column: bool = True,
) -> pd.DatetimeIndex | pd.Series | pd.DataFrame:
    """Normalise timestamp-like Pandas indexes across backends and container types.

    We have several valid ways timestamped market data can appear in memory, and
    they all need to behave identically for downstream code.

    The traditional Pandas path usually gives us a native
    :class:`pandas.DatetimeIndex`, but that is no longer guaranteed once PyArrow
    enters the picture. When parquet data is read with Arrow-backed dtypes, a
    timestamp index may come back as a plain :class:`pandas.Index` whose dtype is
    something like ``timestamp[ns][pyarrow]``. The values are still timestamps,
    yet old code that checks ``isinstance(index, pd.DatetimeIndex)`` will reject
    the result even though timestamp arithmetic, slicing, resampling and plotting
    are conceptually supposed to work.

    There is a second long-standing variation as well: grouped OHLCV and TVL
    data is often stored with a ``MultiIndex`` such as ``(pair_id, timestamp)``
    instead of a single timestamp index. In those cases we only want the
    timestamp level. Finally, some callers still hold an unindexed data frame
    where the timestamp lives in a dedicated column and must be promoted to the
    index before anything time-aware can happen.

    This helper accepts all of those shapes and turns them into one canonical
    representation:

    1. If passed a :class:`pandas.MultiIndex`, extract the timestamp level.
    2. If passed an Arrow-backed timestamp index, coerce it to a native
       :class:`pandas.DatetimeIndex`.
    3. If passed a :class:`pandas.Series`, return a series with a normalised
       timestamp index.
    4. If passed a :class:`pandas.DataFrame`, return a data frame with a
       normalised timestamp index, optionally first promoting ``timestamp_column``
       to become the index.

    The function intentionally preserves the outer container type for series and
    data frames. That lets callers normalise at API boundaries without changing
    the rest of their logic. We are strict when the conversion is not possible:
    a ``TypeError`` is raised instead of silently returning broken data, because
    the surrounding code almost always depends on genuinely time-indexed input.

    :param data:
        Timestamp-like object to normalise. Supported inputs are ``pd.Index``,
        ``pd.Series`` and ``pd.DataFrame``.

    :param timestamp_column:
        For data frames, a column to promote to the index before normalisation if
        the frame is not already indexed by timestamp.

    :param timestamp_level:
        Which level to extract from a ``MultiIndex``. By default we look for a
        level named ``"timestamp"`` and fall back to the last level if that
        level name is absent.

    :param keep_timestamp_column:
        When ``timestamp_column`` is used for a data frame, keep the source
        column in the returned frame instead of dropping it.

    :return:
        ``pd.DatetimeIndex`` for index input, otherwise the original series or
        data frame type with a native ``DatetimeIndex`` attached.
    """

    if isinstance(data, pd.DataFrame):
        if isinstance(data.index, pd.DatetimeIndex):
            return data

        if timestamp_column is not None and timestamp_column in data.columns and not isinstance(data.index, pd.MultiIndex):
            data = data.set_index(timestamp_column, drop=not keep_timestamp_column)

        data = data.copy()
        data.index = normalise_timestamp_index(data.index, timestamp_level=timestamp_level)
        return data

    if isinstance(data, pd.Series):
        index = normalise_timestamp_index(data.index, timestamp_level=timestamp_level)

        if index is data.index:
            return data

        data = data.copy()
        data.index = index
        return data

    index = data

    if isinstance(index, pd.MultiIndex):
        try:
            index = index.get_level_values(timestamp_level)
        except (KeyError, IndexError):
            index = index.get_level_values(-1)

    if not isinstance(index, pd.DatetimeIndex):
        try:
            index = pd.DatetimeIndex(index)
        except Exception as exc:
            raise TypeError(f"Could not convert index to DatetimeIndex: {index}") from exc

    return index


def flatten_dataframe_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure we have a datetime index as the index for the DataFrame.

    - Multipair data sources may have (pair_id, timestamp) index
      instead of (timestamp) index

    :return:
        DataFrame copy with a timestamp-only index
    """

    return normalise_timestamp_index(df)


def get_timestamp_index(df: pd.DataFrame) -> pd.DatetimeIndex:
    """Get DateTimeIndex for pair OHCLV data.
    
    See :py:func:`flatten_dataframe_datetime_index` for comments,

    - Return `df.index` or extract timestamp component from `MultiIndex`

    :return:
        DataFrame copy with a timestamp-only index
    """

    return normalise_timestamp_index(df.index)
