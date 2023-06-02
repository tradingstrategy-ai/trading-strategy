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


def forward_fill(
        df: pd.DataFrame,
        freq: pd.DateOffset,
        columns: Tuple[str] = ("open", "close"),
):
    """Forward-fill data in a multiple trading by GroupBy DataFrame.

    :py:term:`Forward fill` certain candle columns.

    :param df:
        GroupBy DataFrame containing candle data for multiple trading pairs

    :param freq:
        The target frequency for the DataFrame.

    :param columns:
        Columns to fill.

        To save memory and speed, only fill the columns you need.
        Usually "open" and "close" are enough.

    :return:
        DataFrame where each timestamp has a value set for columns.
    """

    assert isinstance(df, pd.DataFrame)
    assert isinstance(freq, pd.DateOffset)

    df = df.resample(freq)

    for column in columns:
        match column:
            case "open":
                df['open'] = df["close"].fillna(method='ffill')
            case "close":
                df['close'] = df["close"].fillna(method='ffill')
                pass
            case _:
                raise NotImplementedError(f"Forward-fill for {column} not yet implemented")
