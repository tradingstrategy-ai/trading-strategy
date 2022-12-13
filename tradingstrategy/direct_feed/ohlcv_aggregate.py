"""OHLCV aggregation function for Pandas


"""

import pandas as pd

from tradingstrategy.direct_feed.timeframe import Timeframe


def ohlcv_resample_trades(
        df: pd.DataFrame,
        timeframe: Timeframe,
) -> pd.DataFrame:
    """Resample incoming "tick" data.

    - The incoming DataFrame is not groupe by pairs yet,
      but presents stream of data from the blockchain.

      It must have columns `pair`, `amount`, `price`, `exchange_rate`.
      It will be indexed by `timestamp` for the operations.

    - There must be a price given externally for the first open

    - Build OHLCV dataframe from individual trades

    - Handle any exchange rate conversion

    - Any missing values will be filled with
      the last valid close

    - Exchange rate resample is the avg exchange rate of the candle.
      Thus, resampled data might not be accurately converted between native quote
      asset and US dollar value. Any currency conversion must happen before.

    ... and be timestamp indexed.

    :param df:
        DataFrame of incoming trades.

        Must have columns `price`, `amount`

    :param freq:
        Pandas frequency string for the candle duration.

        E.g. `1D`

    :param offset:
        Allows you to "shift" candles

    :param conversion:
        Convert the incomign values if needed

    :return:
        OHLCV dataframes that are grouped by pair
    """

    assert len(df) > 0, "Empty dataframe"


    df = df.set_index("timestamp").groupby("pair")

    # Calculate high, low and close values using naive
    # resamping. This assumes there were trades within the freq window.
    df2 = df["price"].resample(timeframe.freq).agg({
        'high': 'max',
        'low': 'min',
        'close': 'last'})

    blocks_df = df["block_number"].resample(timeframe.freq).agg({
        'end_block': 'max',
        'start_block': 'min'})

    df2["exchange_rate"] = df["exchange_rate"].resample(timeframe.freq).mean()
    df2["start_block"] = blocks_df["start_block"]
    df2["end_block"] = blocks_df["end_block"]

    return df2
