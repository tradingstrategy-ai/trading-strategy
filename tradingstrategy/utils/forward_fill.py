"""OHLCV data forward fill.

:term:`Forward fill` missing OHLCV candles in market data feeds.

- Trading Strategy market data feeds are sparse by default,
  to save bandwidth

- DEXes small cap pairs see fewtrades and if there are no trades in a time frame,
  no candle is generated

- Forward-filled data is used on the client side

- We need to forward fill to make price look up, especially for stop losses faster,
  as forward-filled data can do a simple index look up to get a price,
  instead of backwinding to the last available price


"""
from typing import Tuple

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy


def forward_fill(
        df: pd.DataFrame | DataFrameGroupBy,
        freq: pd.DateOffset,
        columns: Tuple[str] = ("open", "close"),
        drop_other_columns=True,
):
    """Forward-fill OHLCV data for multiple trading pairs.

    :py:term:`Forward fill` certain candle columns.

    If multiple pairs are given as a `GroupBy`, then the data is filled
    only for the min(pair_timestamp), max(timestamp) - not for the
    range of the all data.

    .. note ::

        `timestamp` and `pair_id` columns will be deleted in this process
         - do not use these columns, but corresponding indexes instead.

    :param df:
        Candle data for single or multiple trading pairs

        - GroupBy DataFrame containing candle data for multiple trading pairs
          (grouped by column `pair_id`).

        - Normal DataFrame containing candle data for a single pair

    :param freq:
        The target frequency for the DataFrame.

    :param columns:
        Columns to fill.

        To save memory and speed, only fill the columns you need.
        Usually `open` and `close` are enough and also filled
        by default.

    :param drop_other_columns:
        Remove other columns before forward-fill to save memory.

        The resulting DataFrame will only have columns listed in `columns`
        parameter.

        The removed columns include ones like `high` and `low`, but also Trading Strategy specific
        columns like `start_block` and `end_block`. It's unlikely we are going to need
        forward-filled data in these columns.

    :return:
        DataFrame where each timestamp has a value set for columns.
    """

    assert isinstance(df, (pd.DataFrame, DataFrameGroupBy))
    assert isinstance(freq, pd.DateOffset)

    grouped = isinstance(df, DataFrameGroupBy)

    # https://www.statology.org/pandas-drop-all-columns-except/
    if drop_other_columns:
        df = df[list(columns)]

    # Fill missing timestamps with NaN
    # https://stackoverflow.com/a/45620300/315168
    df = df.resample(freq).mean()

    columns = set(columns)

    # We always need to ffill close first
    for column in ("close", "open", "high", "low"):
        if column in columns:
            columns.remove(column)

            match column:
                case "close":
                    # Sparse close is the previous close
                    df["close"] = df["close"].fillna(method="ffill")
                case "open" | "high" | "low":
                    # Fill open, high, low from the ffill'ed close.
                    df[column] = df[column].fillna(df["close"])

    if columns:
        # Unprocessable columns left
        raise NotImplementedError(f"Does not know how to forward fill: {columns}")

    # Regroup by pair, as this was the original data format
    if grouped:
        df = df.groupby("pair_id")

    return df
