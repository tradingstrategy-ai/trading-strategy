"""Float and currency conversion for data feeds."""
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
        Dataframe with floating point USD pricing.


        The resulting dataframe is still indexed by block.
    """

    df2 = pd.DataFrame(index=df.index)
    df2["pair"] = df["pair"]
    df2["timestamp"] = df["timestamp"]
    df2["block_number"] = df["block_number"]
    df2["exchange_rate"] = df["exchange_rate"].astype(float)
    if conversion == CurrencyConversion.us_dollar:
        df2["price"] = df["price"].astype(float) * df2["exchange_rate"]
        df2["amount"] = df["amount"].astype(float) * df2["exchange_rate"]
    else:
        df2["price"] = df["price"].astype(float)
        df2["amount"] = df["amount"].astype(float)
    return df2
