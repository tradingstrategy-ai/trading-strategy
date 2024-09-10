"""Wrangle incoming data.

- Wrangle is a process where we massage incoming price/liquidity data for the isseus we may have encountered during the data collection

- Common DEX data issues are absurd price high/low spikes due to MEV trades

- We also have some open/close values that are "broken" in a sense that they do not reflect the market price you would be able to trade,
  again likely due to MEV

- See :py:func:`fix_dex_price_data` for fixing

"""
import logging
import datetime
from itertools import islice

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy
import numpy as np

from .time import naive_utcnow
from .forward_fill import forward_fill as _forward_fill
from ..pair import PandasPairUniverse

logger = logging.getLogger(__name__)


def fix_bad_wicks(
    df: pd.DataFrame,
    threshold=(0.1, 1.9),
    too_slow_threshold=15,
    verbose=False,
    bad_open_close_threshold: float | None=3.0,
) -> pd.DataFrame:
    """Correct out bad high/low values in OHLC data.

    Applicable for both :term:`OHLCV` price feeds and liquidity feeds.

    On :term:`Uniswap` v2 and compatibles, Bad wicks are caused by e.g. very large flash loan, oracle price manipulation attacks,
    and misbheaving bots.

    This function removes bad high/low values and sets them to open/close if they seem to be wildly out of sample.

    :param threshold:
        How many pct % wicks are allowed through.

        Tuple (low threshold, high threshold) relative to close.

        Default to 50%. A high wick cannot be more than 50% of close.

    :param too_slow_threshold:
        Complain if this takes too long

    :param bad_open_close_threshold:
        How many X open must be above the high to be considered a broken data point.

        The open price will be replaced with high price.

        Do not set for liquidity processing.


    :param verbose:
        Make some debug logging when using the function for manual data diagnostics.

    """

    start = naive_utcnow()

    if len(df) == 0:
        return df

    # Optimised with np.where()
    # https://stackoverflow.com/a/65729035/315168
    if threshold is not None:
        df["high"] = np.where(df["high"] > df["close"] * threshold[1], df["close"], df["high"])
        df["low"] = np.where(df["low"] < df["close"] * threshold[0], df["close"], df["low"])

    # For manual diagnostics tracking down bad trading pair data
    if verbose and bad_open_close_threshold:
        bad_opens = df[df["open"] > df["high"] * bad_open_close_threshold]
        for idx, row in islice(bad_opens.iterrows(), 10):
            logger.warning(
                "Pair id %d, timestamp: %s, open: %s, high: %s, buy volume: %s sell volume: %s, volume: %s",
                row.pair_id,
                row.timestamp,
                row.open,
                row.high,
                row.get("buy_volume"),
                row.get("sell_volume"),
                row.get("volume"),
            )
        logger.warn("Total %d bad open price entries detected", len(bad_opens))

    # Issues in open price values with data point - open cannot be higher than high.
    # Not strickly "wicks" but we fix all data while we are at it.
    if bad_open_close_threshold:
        df["open"] = np.where(df["open"] > df["high"] * bad_open_close_threshold, df["high"], df["open"])
        df["close"] = np.where(df["close"] > df["high"] * bad_open_close_threshold, df["high"], df["close"])

    duration = naive_utcnow() - start

    if duration > datetime.timedelta(seconds=too_slow_threshold):
        logger.warning("Very slow fix_bad_wicks(): %s", duration)

    # The following code chokes
    # mask = (df["high"] > df["close"] * (1+threshold)) | (df["low"] < df["close"] * threshold)
    #df.loc[mask, "high"] = df["close"]
    #df.loc[mask, "low"] = df["close"]
    #df.loc[mask, "wick_filtered"] = True
    return df


def filter_bad_wicks(df: pd.DataFrame, threshold=(0.1, 1.9)) -> pd.DataFrame:
    """Mark the bad wicks.

    On :term:`Uniswap` v2 and compatibles, Bad wicks are caused by e.g. very large flash loan, oracle price manipulation attacks,
    and misbheaving bots.

    This function removes bad high/low values and sets them to open/close if they seem to be wildly out of sample.

    :param threshold:
        How many pct % wicks are allowed through as (low, high) tuple.

        This is a tuple (low threshold, high threshold).

        If low < close * threshold[0] ignore the value.

        If high > close * threshold[0] ignore the value.

    """

    df_matches = df.loc[
        (df["high"] > df["close"] * threshold[1]) | (df["low"] < df["close"] * threshold[0])
    ]

    return df_matches


def remove_zero_candles(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Remove any candle that has a zero value for OHLC

    :param df: Dataframe that may contain zero candles
    :return: pd.Dataframe
    """
    if len(df) > 0:
        filtered_df = df[(df['open'] != 0) & (df['high'] != 0) & (df['low'] != 0) & (df['close'] != 0)]
        return filtered_df
    return df


def fix_dex_price_data(
    df: pd.DataFrame | DataFrameGroupBy,
    freq: pd.DateOffset | str | None = None,
    forward_fill: bool = True,
    bad_open_close_threshold: float | None = 3.0,
    fix_wick_threshold: tuple | None = (0.1, 1.9),
    remove_candles_with_zero: bool = True,
) -> pd.DataFrame:
    """Wrangle DEX price data for all known issues.

    - Fix broken open/high/low/close value so that they are less likely to cause problems for algorithms

    - Wrangle is a process where we massage incoming price/liquidity data for the isseus we may have encountered during the data collection

    - Common DEX data issues are absurd price high/low spikes due to MEV trades

    - We also have some open/close values that are "broken" in a sense that they do not reflect the market price you would be able to trade,
      again likely due to MEV

    Example:

    .. code-block:: python

          # After we know pair ids that fill the liquidity criteria,
          # we can build OHLCV dataset for these pairs
          print(f"Downloading/opening OHLCV dataset {time_bucket}")
          price_df = client.fetch_all_candles(time_bucket).to_pandas()
          print(f"Filtering out {len(top_liquid_pair_ids)} pairs")
          price_df = price_df.loc[price_df.pair_id.isin(top_liquid_pair_ids)]

          print("Wrangling DEX price data")
          price_df = price_df.set_index("timestamp", drop=False).groupby("pair_id")
          price_df = fix_dex_price_data(
              price_df,
              freq=time_bucket.to_frequency(),
              forward_fill=True,
          )

          print(f"Retrofitting OHLCV columns for human readability")
          price_df = price_df.obj
          price_df["pair_id"] = price_df.index.get_level_values(0)
          price_df["ticker"] = price_df.apply(lambda row: make_full_ticker(pair_metadata[row.pair_id]), axis=1)
          price_df["link"] = price_df.apply(lambda row: make_link(pair_metadata[row.pair_id]), axis=1)

          # Export data, make sure we got columns in an order we want
          print(f"Writing OHLCV CSV")
          del price_df["timestamp"]
          del price_df["pair_id"]
          price_df = price_df.reset_index()
          column_order = ('ticker', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'link', 'pair_id',)
          price_df = price_df.reindex(columns=column_order)  # Sort columns in a specific order
          price_df.to_csv(
            price_output_fname,
          )
          print(f"Wrote {price_output_fname}, {price_output_fname.stat().st_size:,} bytes")

    :param df:
        Price dataframe with OHLCV data.

        May contain columns named open, close, high, low, volume and timestamp.

        For multipair data this must be `DataFrameGroupBy`.

    :param freq:
        The incoming Pandas frequency of the data, e.g. "d" for daily.

        If the incoming data frequency and `freq` parameter do not match, the data is resampled o the given frequency.

    :param fix_wick_threshold:
        Apply abnormal high/low wick fix filter.

        Percent value of maximum allowed high/low wick relative to close.
        By default fix values where low is 90% lower than close and high is 90% higher than close.

        See :py:func:`~tradingstrategy.utils.groupeduniverse.fix_bad_wicks` for more information.

    :param bad_open_close_threshold:
        See :py:func:`fix_bad_wicks`.

    :param primary_key_column:
        The pair/reserve id column name in the dataframe.

    :param remove_zero_candles:
        Remove candles with zero values for OHLC.

        To deal with abnormal data.

    :param forward_fill:
        Forward-will gaps in the data.

        Forward-filling data will delete any unknown columns,
        see :py:func:`tradingstrategy.utils.forward_fill.forward_fill` details.

    :return:
        Fixed data frame.

        If forward fill is used, all other columns outside OHLCV are dropped.
    """

    assert isinstance(df, (pd.DataFrame, DataFrameGroupBy)), f"Got: {df.__class__}"

    if isinstance(df, DataFrameGroupBy):
        raw_df = df.obj
    else:
        raw_df = df

    if fix_wick_threshold or bad_open_close_threshold:
        logger.info("Fixing bad wicks")
        raw_df = fix_bad_wicks(
            raw_df,
            fix_wick_threshold,
            bad_open_close_threshold=bad_open_close_threshold,
        )

    if remove_candles_with_zero:
        logger.info("Fixing zero volume candles")
        raw_df = remove_zero_candles(raw_df)

    if forward_fill:
        logger.info("Forward filling price data")
        assert freq, "freq argument must be given if forward_fill=True"
        df = _forward_fill(df, freq)
        return df
    else:
        return raw_df


def examine_anomalies(
    pair_universe: PandasPairUniverse | None,
    price_df: pd.DataFrame,
    printer=lambda x: print(x),
    max_print=5,
    pair_id_column="pair_id",
    open_close_max_diff=0.99,
):
    """Check the price dataframe for data issues.

    - Print out to consoles bad rows in the OHLCV candle price data

    TODO: This is a work in progress helper.
    """

    issues_found = False

    # Find zero prices
    zero_prices = price_df.loc[price_df["open"] <= 0]
    zero_prices = zero_prices.drop_duplicates(subset=pair_id_column, keep='first')

    for zero_price_entry in zero_prices.iloc[0:max_print].iterrows():
        printer(f"Found zero price entry {zero_price_entry}")
        issues_found = True

    open_close_mask = (((price_df["close"] - price_df["open"]) / price_df["open"]).abs() >= open_close_max_diff)
    open_close_gap = price_df.loc[open_close_mask]
    for open_close_entry in open_close_gap.iloc[0:max_print].iterrows():
        diff = open_close_entry["close"] / open_close_entry["open"]
        printer(f"Found open/close diff {diff} at\n{open_close_entry}")
        issues_found = True

    if not issues_found:
        printer(f"No data issues found, {len(price_df)} rows analysed")

    return issues_found

