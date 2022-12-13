"""OHLCV aggregation function for Pandas


"""
import enum

import pandas as pd


class CurrencyConversion(enum.Enum):
    """How prices in the OHLCV output are handled."""

    #: Everything is converted to US dollars
    us_dollar = "us_dollar"

    #: Everything is kept in its native crypto token.
    #:
    #: Nothing is done for the prices
    crypto_quote_token = "quote_token"


def convert_to_float(df: pd.DataFrame, conversion: CurrencyConversion) -> pd.DataFrame:
    """Convert raw transaction values ot USD before resample.

    - This will create a copy of the dataframe.

    - The resulting DataFrame is going to lose columns
      that are not on its retained list

    - Columns are going to be converted to floats

    :param df:
        Raw trades dataframe

    :param conversion:
        Do we want the resulting dataframe to be converted
        using the given exchange rate

    :return:
        Dataframe with floating point USD pricing
    """

    df2 = pd.DataFrame()
    df2.index = df.index
    df2["pair"] = df["pair"]
    df2["block_number"] = df["block_number"]
    df2["exchange_rate"] = df["exchange_rate"].astype(float)
    if conversion == CurrencyConversion.us_dollar:
        df2["price"] = df["price"].astype(float) * df2["exchange_rate"]
        df2["amount"] = df["amount"].astype(float) * df2["exchange_rate"]
    else:
        df2["price"] = df["price"].astype(float)
        df2["amount"] = df["amount"].astype(float)
    return df2


def ohlcv_resample_tick_data(
        df: pd.DataFrame,
        freq: str = "1D",
        offset: pd.Timedelta = pd.Timedelta(seconds=0)):
    """Resample incoming "tick" data.

    - The incoming DataFrame is not groupe by pairs yet,
      but presents stream of data from the blockchain

    - Build OHLCV dataframe from individual trades

    - Handle any exchange rate conversion

    `df` must have columns

    - `amount`

    ` `volume`

    ... and be timestamp indexed.

    :param df:
        DataFrame of incoming trades

    :param freq:
        Pandas frequency string for the candle duration.

        E.g. `1D`

    :param offset:
        Allows you to "shift" candles

    :param conversion:
        Convert the incomign values if needed

    :return:
        OHLCV dataframe
    """

    naive_resample = df["transaction"].resample("1D") .agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'})
