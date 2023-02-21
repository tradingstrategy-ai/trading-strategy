"""OHLCV aggregation function for Pandas


"""
from dataclasses import dataclass

import pandas as pd

from tradingstrategy.direct_feed.timeframe import Timeframe
from tradingstrategy.direct_feed.direct_feed_pair import PairId


@dataclass(slots=True, frozen=True)
class OHLCVCandle:
    """One OHLCV candle in the direct data feed."""

    pair: PairId
    timestamp: pd.Timestamp
    start_block: int
    end_block: int

    open: float
    high: float
    low: float
    close: float
    volume: float
    exchange_rate: float

    @staticmethod
    def get_dataframe_columns() -> dict:
        fields = dict([
            ("pair", "string"),
            ("start_block", "uint64"),
            ("end_block", "uint64"),
            ("open", "float32"),
            ("high", "float32"),
            ("low", "float32"),
            ("close", "float32"),
            ("volume", "float32"),
            ("exchange_rate", "float32"),
        ])
        return fields


def resample_trades_into_ohlcv(
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

        Pandas frequency string for the new timeframe duration.

        E.g. `D` for daily.

        See https://stackoverflow.com/a/35339226/315168

    :param offset:
        Allows you to "shift" candles

    :param conversion:
        Convert the incomign values if needed

    :return:
        OHLCV dataframes that are grouped by pair.

        Open price is not the price of the previous close,
        but the first trade done within the timeframe.
    """

    assert len(df) > 0, "Empty dataframe"

    df = df.set_index("timestamp")

    df["abs_amount"] = df["amount"].abs()

    df = df.groupby("pair")

    # Calculate high, low and close values using naive
    # resamping. This assumes there were trades within the freq window.
    df2 = df["price"].resample(timeframe.freq).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'})

    # Retain timestamp as a column as well,
    # because resample() drops it
    df2["timestamp"] = df2.index.get_level_values(1)

    # TODO: Integers here get converted to floats when processed with agg().
    # Figure out how to prevent this.
    blocks_df = df["block_number"].resample(timeframe.freq).agg({
        'end_block': 'max',
        'start_block': 'min'})

    volume_df = df["abs_amount"].resample(timeframe.freq).agg({
        "total_volume": "sum",
        "avg_amount": "mean",
    })

    trade_side_df = df["amount"].resample(timeframe.freq).agg({
        'buys': lambda x: (x > 0).sum(),
        'sells': lambda x: (x < 0).sum()})

    df2["exchange_rate"] = df["exchange_rate"].resample(timeframe.freq).mean()
    df2["start_block"] = blocks_df["start_block"]
    df2["end_block"] = blocks_df["end_block"]
    df2["volume"] = volume_df["total_volume"]
    df2["avg_trade"] = volume_df["avg_amount"]
    df2["buys"] = trade_side_df["buys"]
    df2["sells"] = trade_side_df["sells"]

    # Resample generates NaN values instead of sparse data.
    # In this point, we just drop the rows that have any NaNs in them

    #                        high     low   close  exchange_rate  start_block  end_block
    # pair     timestamp
    # AAVE-ETH 2020-01-01    80.0    80.0    80.0         1600.0          1.0        1.0
    #          2020-01-02    96.0    96.0    96.0         1600.0          2.0        2.0
    # ETH-USDC 2020-01-02  1600.0  1600.0  1600.0            1.0          2.0        2.0
    #          2020-01-03     NaN     NaN     NaN            NaN          NaN        NaN
    #          2020-01-04     NaN     NaN     NaN            NaN          NaN        NaN
    #          2020-01-05  1620.0  1400.0  1400.0            1.0          7.0        8.0

    df2 = df2.dropna()
    return df2


def get_feed_for_pair(df: pd.DataFrame, pair: PairId) -> pd.DataFrame:
    """Get candles for a single pair.

    :param df:
        An OHLCV dataframe.

        Generated with :py:func:`ohlcv_resample_trades`

    :param pair:
        Which pair we are interested in

    :return:
        Dataframe for this pair
    """

    # groupby().resample() produces multi-index
    # where the first index is the pair

    # ipdb> df.index
    # MultiIndex([('AAVE-ETH', '2020-01-01'),
    #             ('AAVE-ETH', '2020-01-02'),
    #             ('ETH-USDC', '2020-01-02'),
    #             ('ETH-USDC', '2020-01-05')],
    #            names=['pair', 'timestamp'])

    # No data, return empty dataframe
    if len(df) == 0:
        return pd.DataFrame()

    # https://stackoverflow.com/a/45563615/315168
    try:
        return df.xs(pair)
    except KeyError as e:
        raise KeyError(f"Could not find pair for address {pair}") from e


def truncate_ohlcv(df: pd.DataFrame, ts: pd.Timestamp) -> pd.DataFrame:
    """Clear the chain tip data to be written again.

    :param ts:
        Drop everything after this (inclusive).
    """
    if len(df) == 0:
        return df
    df = df[df.index.get_level_values('timestamp') < ts]
    return df

