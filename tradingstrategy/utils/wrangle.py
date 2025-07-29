"""Wrangle DEX market data to be better suitable for algorithmic trading.

- Wrangle is a process where we "massage" incoming price/liquidity data for the isseus we may have encountered during the data collection

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
from ..types import AnyTimestamp

logger = logging.getLogger(__name__)


#: Floating point danger zone for price values
DEFAULT_MIN_MAX_RANGE = (0.00000001, 1_000_000.0)


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


def remove_min_max_price(
    df: pd.DataFrame,
    min_max_price: tuple = DEFAULT_MIN_MAX_RANGE,
) -> pd.DataFrame:
    """Remove candles where open value is outside the floating point range detector.

    :param df:
        Dataframe that may contain open or close values for too funny precision.

    :param min_max_price:
        Min and max price allowed for open/close before dropping.

    :return: pd.Dataframe
    """
    if len(df) > 0:
        mask = (df["open"] < min_max_price[0]) | (df["close"] < min_max_price[0]) | \
               (df["open"] > min_max_price[1]) | (df["close"] > min_max_price[1])

        filtered_df = df[~mask]
        return filtered_df
    return df


def _replace_for_groups(group, replacement_dict):
    group_name = group.name
    if group_name in replacement_dict:
        for col, value in replacement_dict[group_name].items():
            group[col] = value
    return group


def fix_prices_in_between_time_frames(
    dfgb: pd.DataFrame | DataFrameGroupBy,
    fix_inbetween_threshold: tuple | None = (-0.99, 5.0),
    pair_id_column="pair_id",
):
    """Fix MEV bots breaking open/price.

    TODO: This needs to be later fixed at the data collection level, MEV transactions masked out.

    Example daily candle with a broken open price:

    .. code-block:: text

        COMP-WETH on Uniswap v2 on Ethereum

        2023-12-10 23:00:00	2023-12-10 23:00:00	56.253001	56.253001	55.948422	55.948422	348.915474	2373.049643	0.0	3.0	0.000000	348.915474	1015	18758972	18759006	NaN
        2023-12-11 00:00:00	2023-12-11 00:00:00	0.363204	55.181369	0.354456	55.181369	180421.431179	2357.352148	1.0	6.0	90162.684736	90258.746443	1015	18759541	18759541	NaN
        2023-12-11 02:00:00	2023-12-11 02:00:00	50.059992	50.612159	50.059992	50.612159	2859.011552	2241.581295	1.0	1.0	628.775186	2230.236366	1015	18759912	18760129	NaN

    `Underlying MEV TX causing the issue <https://etherscan.io/tx/0x1106418384414ed56cd7cbb9fedc66a02d39b663d580abc618f2d387348354ab>`__, ChatGPT formatted:

    .. code-block:: text

        +--------------------------------------+-------------------------------------+-------------------+-------------+-------------------+-------------------------------------+-------------------+-------------+-------------+
        |               Action                 |          Swap Amount (Token 1)      |   Value (Token 1)  |  Token 1    |    For (Token 2)   |          Swap Amount (Token 2)      |   Value (Token 2)  |  Token 2    |  Platform   |
        +--------------------------------------+-------------------------------------+-------------------+-------------+-------------------+-------------------------------------+-------------------+-------------+-------------+
        | Aggregated Swap of 3 Tokens           |                                     |                   |             |                   |                                     |                   |             |             |
        | Swap                                 | 20,096.81048332570788104            | $956,407.21       | COMP        | ETH               | 38.247439949510380362              | $101,436.63       | ETH         | Uniswap V2  |
        | Swap                                 | 65.583941528193262325               | $3,121.14         | COMP        | ETH               | 0.010044352430924858               | $26.64            | ETH         | Uniswap V2  |
        | Swap                                 | 0.010044352430924858                | $26.64            | ETH         | DAI               | 23.587216376218824815              | $23.58            | DAI         | Uniswap V2  |
        | Swap                                 | 65.780693352777842112               | $3,130.50         | COMP        | ETH               | 0.010014399362431825               | $26.56            | ETH         | Uniswap V2  |
        | Swap                                 | 0.010014399362431825                | $26.56            | ETH         | DAI               | 23.516703895990702452              | $23.51            | DAI         | Uniswap V2  |
        | Swap                                 | 65.978035432836175638               | $3,139.89         | COMP        | ETH               | 0.009984535616403163               | $26.48            | ETH         | Uniswap V2  |
        | Swap                                 | 0.009984535616403163                | $26.48            | ETH         | DAI               | 23.446402724807108015              | $23.44            | DAI         | Uniswap V2  |
        | Swap                                 | 66.175969539134684165               | $3,149.31         | COMP        | ETH               | 0.009954760926472085               | $26.40            | ETH         | Uniswap V2  |
        | Swap                                 | 0.009954760926472085                | $26.40            | ETH         | DAI               | 23.376312226345838338              | $23.37            | DAI         | Uniswap V2  |
        | Swap                                 | 5.013744524035853058                | $238.60           | COMP        | ETH               | 0.000751785040201655               | $1.99             | ETH         | Uniswap V2  |
        | Swap                                 | 0.000751785040201655                | $1.99             | ETH         | DAI               | 1.765375649239336674               | $1.76             | DAI         | Uniswap V2  |
        | Swap                                 | 38.247439949510380362               | $101,436.63       | ETH         | COMP              | 20,353.762314725950969279          | $968,635.55       | COMP        | Uniswap V2  |
        | Swap                                 | 0.149285130679667947                | $395.92           | ETH         | COMP              | 4,200                              | $199,878.00       | COMP        | Sushiswap   |
        +--------------------------------------+-------------------------------------+-------------------+-------------+-------------------+-------------------------------------+-------------------+-------------+-------------+

    :param dfgb:
        Assume grouped by pair_id and MultiLevel index (pair_id, timestamp).

    """
    assert isinstance(dfgb, DataFrameGroupBy), f"Currently only implemented for DataFrameGroupBy"

    replacements = {}

    for pair_id, price_df in dfgb:
        healed_ohlcv_df = heal_anomalies(
            price_df,
            low_diff=fix_inbetween_threshold[0],
            high_diff=fix_inbetween_threshold[1],
            hint=f"pair {pair_id}"
        )
        if healed_ohlcv_df is not None:
            logger.info("Healed OHLCV data for pair %s - detected issues", pair_id)
            replacements[pair_id] = healed_ohlcv_df

    healed = dfgb.apply(lambda x: _replace_for_groups(x, replacements), include_groups=True)
    healed = healed.set_index("timestamp", drop=False)
    return healed.groupby(pair_id_column)


def fix_dex_price_data(
    df: pd.DataFrame | DataFrameGroupBy,
    freq: pd.DateOffset | str | None = None,
    forward_fill: bool = True,
    bad_open_close_threshold: float | None = 3.0,
    fix_wick_threshold: tuple | None = (0.1, 1.9),
    fix_inbetween_threshold: tuple | None = (-0.99, 5.0),
    min_max_price: tuple | None = DEFAULT_MIN_MAX_RANGE,
    remove_candles_with_zero_volume: bool = True,
    pair_id_column="pair_id",
    forward_fill_until: AnyTimestamp | None = None,
) -> pd.DataFrame:
    """Wrangle DEX price data for all known issues.

    - Fix broken open/high/low/close value so that they are less likely to cause problems for algorithms

    - Wrangle is a process where we massage incoming price/liquidity data for the isseus we may have encountered during the data collection

    - Common DEX data issues are absurd price high/low spikes due to MEV trades

    - We also have some open/close values that are "broken" in a sense that they do not reflect the market price you would be able to trade,
      again likely due to MEV

    - Before calling this, you want to call :py:func:`normalise_volume` for OHLCV data

    Example:

    .. code-block:: python

          from tradingstrategy.utils.wrangle import fix_dex_price_data

          # After we know pair ids that fill the liquidity criteria,
          # we can build OHLCV dataset for these pairs
          print(f"Downloading/opening OHLCV dataset {time_bucket}")
          price_df = client.fetch_all_candles(time_bucket).to_pandas()
          print(f"Filtering out {len(top_liquid_pair_ids)} pairs")
          price_df = price_df.loc[price_df.pair_id.isin(top_liquid_pair_ids)]

          print("Wrangling DEX price data")
          price_df = price_df.set_index("timestamp", drop=False)

          # Normalise volume datapoints
          price_df = normalise_volume(price_df)

          # Conver to grouped data
          price_dfgb = price_df.groupby("pair_id")

          price_dfgb = fix_dex_price_data(
              price_dfgb,
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

    :param pair_id_column:
        The pair/reserve id column name in the dataframe.

    :param remove_candles_with_zero_volume:
        Remove candles with zero values for OHLC.

        To deal with abnormal data.

    :param min_max_price:
        Remove candles where open value is outside the floating point range detector.

        See :py:func:`remove_min_max_price`.

    :param forward_fill:
        Forward-will gaps in the data.

        Forward-filling data will delete any unknown columns,
        see :py:func:`tradingstrategy.utils.forward_fill.forward_fill` details.

    :param forward_fill_until:
        The timestamp which we know the data is valid for.

        If there are price gaps at rarely traded pairs at the end of the (live) OHLCV series,
        we will forward fill the data until this timestamp.

        If not given forward fills until the last trade of the pair.

    :return:
        Fixed data frame.

        If forward fill is used, all other columns outside OHLCV are dropped.
    """

    assert isinstance(df, (pd.DataFrame, DataFrameGroupBy)), f"Got: {df.__class__}"

    if isinstance(df, DataFrameGroupBy):
        raw_df = df.obj
    else:
        raw_df = df

    if min_max_price:
        logger.info("Removing open/close prices outside our min-max tolerance range: %s", min_max_price)
        raw_df = remove_min_max_price(
            raw_df,
            min_max_price=min_max_price,
        )
        raw_df = raw_df.copy()  # Need to mutate in fix_bad_wicks(), cannot be view

    if fix_wick_threshold or bad_open_close_threshold:
        logger.info("Fixing bad wicks, fix_wick_threshold:%s, bad_open_close_threshold: %s", fix_wick_threshold, bad_open_close_threshold)
        raw_df = fix_bad_wicks(
            raw_df,
            fix_wick_threshold,
            bad_open_close_threshold=bad_open_close_threshold,
        )

    if remove_candles_with_zero_volume:
        logger.info("Fixing zero volume candles")
        raw_df = remove_zero_candles(raw_df)

    # For the further cleanup, need to regroup the DataFrame
    if isinstance(df, DataFrameGroupBy):

        logger.info("Regrouping fixed price data")

        # Need to group here
        # TODO: Make this smarter, but how? Read index data in groupby instance?
        assert "timestamp" in raw_df.columns, f"Got {raw_df.columns}"

        regrouped = raw_df.set_index("timestamp", drop=False).groupby(pair_id_column, group_keys=True)

        if fix_inbetween_threshold:

            logger.info("Fixing prices having bad open/close values between timeframes: %s", fix_inbetween_threshold)

            ff_df = fix_prices_in_between_time_frames(
                regrouped,
                fix_inbetween_threshold=fix_inbetween_threshold,
                pair_id_column=pair_id_column,
            )
        else:
            logger.info("Skipped fix_prices_in_between_time_frames()")
            ff_df = regrouped

    else:
        assert not fix_inbetween_threshold, "fix_inbetween_threshold() only works for DataFrameGroupBy input. Set fix_inbetween_threshold == None if you really want to call this"
        ff_df = raw_df

    if forward_fill:

        if isinstance(forward_fill_until, datetime.datetime):
            forward_fill_until = pd.Timestamp(forward_fill_until)

        logger.info("Forward filling OHLCV data, until %s, freq %s", forward_fill_until, freq)
        assert freq, "freq argument must be given if forward_fill=True"

        columns = ff_df.obj.columns if isinstance(ff_df, DataFrameGroupBy) else ff_df.columns
        if "volume" in columns:
            # Price
            df = _forward_fill(ff_df, freq, forward_fill_until=forward_fill_until)
        else:
            # TVL
            df = _forward_fill(ff_df, freq, forward_fill_until=forward_fill_until, columns=("open", "high", "low", "close"))
        return df
    else:
        return ff_df


def examine_price_between_time_anomalies(
    price_series: pd.Series,
    high_diff=5.00,
    low_diff=-0.99,
    column: str = "close",
    heal=False,
) -> pd.DataFrame | pd.Series:
    """Find bad open/close prices where the open price is very different from the value of previous and next day.

    - Must be done pair-by-pair

    TODO: Work in progress.

    :param price_series:
        Incoming OHLCV or single price series.

    :param high_diff:
        A price between days cannot be higher than this multiplier.

    :param low_diff:
        A price between days cannot be lower than this multiplier.

    :param column:
        Column name to check.

        Only relevant if input is DataFrame.

    :param heal:
        IF set return the healed price data witih avg values replacing the outliers.

    :return:
        Diagnostics results as DataFrame.

        If `heal` is set, return healed price data or None if no healing needed.
    """
    assert isinstance(price_series, (pd.Series, pd.DataFrame)), f"Got: {price_series.__class__}"

    if isinstance(price_series, pd.Series):
        df = pd.DataFrame({
            "price": price_series,
        })
    else:
        df = price_series
        df["price"] = df[column]

    df = df.copy()

    # Calculate surrounding price
    df["price_before"] = df['price'].shift(1)
    df["price_after"] = df['price'].shift(-1)
    df['surrounding_avg'] = (df["price_after"] + df["price_before"]) / 2

    # How much the current price is different from the surrounding avg price
    df["price_diff"] = ((df["surrounding_avg"] - df['price']) / df['price'])

    # Create mask for rows where we detect anomalies
    df["high_anomaly"] = df["price_diff"] > high_diff
    df["low_anomaly"] = df["price_diff"] < low_diff
    df["anomaly"] =  df["low_anomaly"] | df["high_anomaly"]

    if heal:
        # Only execute if we have data to heal
        if df["anomaly"].sum() > 0:  # Count of True values of anomalies
            df["fixed_price"] = np.where(df["anomaly"], df["surrounding_avg"], df["price"])
            return df["fixed_price"]
        else:
            return None
    else:
        # Get the rows that are anomalies
        # Can't have value at start and end,
        # drop NaNs
        df = df.dropna(subset=["surrounding_avg"])

        return df.loc[df["anomaly"]]


def heal_anomalies(
    ohlcv_df: pd.DataFrame,
    high_diff=5.00,
    low_diff=-0.99,
    indication_column: str = "close",
    hint="<unknown pair>",
) -> pd.DataFrame | None:
    """Fix bad open/close/high/low prices where the open price is very different from the value of previous and next day.

    - Fix columns open/high/low/close/volume

    - Caused by MEV trades generating spikes and volume

    - If we detect bad candle with MEV trades in it dominating close price, blend the values from previous candles

    TODO: Work in progress.

    :param ohlcv_df:
        Incoming OHLCV or single price series.

    :param high_diff:
        A price between days cannot be higher than this multiplier.

    :param low_diff:
        A price between days cannot be lower than this multiplier.

    :param indication_column:
        Column name to check.

        Only relevant if input is DataFrame.

    :return:
        The same DataFrame with OHLCV columns manipulated. `None` if nothing was done.

        New flag column `healed` added to mark rows we manipulated.

    """
    assert isinstance(ohlcv_df, (pd.Series, pd.DataFrame)), f"Got: {ohlcv_df.__class__}"

    # Calculation dataframe
    df = pd.DataFrame({
        "price": ohlcv_df[indication_column],
    })

    # Calculate surrounding price
    df["pct_change"] = df['price'].pct_change()
    df["change_before"] = df['pct_change'].shift(1)
    df["change_after"] = df['pct_change'].shift(-1)

    # How much the current price is different from the surrounding avg price
    # df["price_diff"] = ((df["surrounding_avg"] - df['price']) / df['price'])

    # Create mask for rows where we detect anomalies
    # high anomaly = price shoots up
    # low  anomaly = price shoots down
    df["high_anomaly"] = (df["pct_change"] > high_diff) & (df["change_after"] < low_diff)
    df["low_anomaly"] = (df["pct_change"] < low_diff) & (df["change_after"] > high_diff)
    df["anomaly"] =  df["low_anomaly"] | df["high_anomaly"]

    count = df["anomaly"].sum()

    if count == 0:
        # Avoid extra work
        return None

    logger.info("Detected %d anomalies for %s", count, hint)

    # display(df.loc[df["anomaly"] == True])

    # Heal anomalies by using avg price and previous volcd
    df['surrounding_avg'] = (df["price"].shift(1) + df["price"].shift(-1)) / 2
    heal_mask = df["anomaly"] == True
    ohlcv_df.loc[heal_mask, "open"] = df['surrounding_avg']
    ohlcv_df.loc[heal_mask, "close"] = df['surrounding_avg']
    ohlcv_df.loc[heal_mask, "high"] = df['surrounding_avg']
    ohlcv_df.loc[heal_mask, "low"] = df['surrounding_avg']
    if "volume" in ohlcv_df.columns:
        ohlcv_df.loc[heal_mask, "volume"] = ohlcv_df.shift(1)["volume"]
    ohlcv_df.loc[heal_mask, "healed"] = True
    return ohlcv_df


def examine_anomalies(
    pair_universe: PandasPairUniverse | None,
    price_df: pd.DataFrame,
    printer=lambda x: print(x),
    max_print=2,
    pair_id_column: str | None="pair_id",
    open_close_max_diff=5.00,
    open_close_min_diff=-0.99,
    between_high_diff: float | None=5.00,
    between_low_diff: float | None=-0.99,
):
    """Check the price dataframe for data issues.

    - Print out to consoles bad rows in the OHLCV candle price data

    Perform

    - Open/close diff check

    - In between timeframes diff check

    TODO: This is a work in progress helper.

    See also:

    - :py:func:`examine_price_between_time_anomalies`

    :param price_df:
        OHLCV data for multiple trading pairs.

        Can be grouped by `pair_id_column`.

    :param pair_id_column:
        Fix column identifies the pair name in the data.

    :param max_print:
        How many entries print per each anomaly check

    :param open_close_max_diff:
        Abnormal price increase X

    :param open_close_min_diff:
        Abnormal price decrease X
    """

    assert isinstance(price_df, pd.DataFrame)

    issues_found = False

    # Find zero prices
    zero_prices = price_df.loc[price_df["open"] <= 0]

    if pair_id_column:
        zero_prices = zero_prices.drop_duplicates(subset=pair_id_column, keep='first')

    if len(zero_prices) > 0:
        printer(f"Total {len(zero_prices)} rows with zero price entry gap")

    for zero_price_entry in zero_prices.iloc[0:max_print].iterrows():
        printer(f"Found zero price entry {zero_price_entry}")
        issues_found = True

    # Find abnormal price jumps within intraday open->close is very different
    open_close_mask = ((price_df["close"] - price_df["open"]) / price_df["open"]) >= open_close_max_diff
    open_close_mask = open_close_mask | (((price_df["close"] - price_df["open"]) / price_df["open"]) <= open_close_min_diff)

    open_close_gap = price_df.loc[open_close_mask]

    if len(open_close_gap) > 0:
        printer(f"Total {len(open_close_gap)} rows with open/close price gap")

    if pair_id_column:
        open_close_gap = open_close_gap.drop_duplicates(subset=pair_id_column, keep='first')

    if len(open_close_gap) > 0:
        for idx, open_close_entry in open_close_gap.iloc[0:max_print].iterrows():
            diff = (open_close_entry["close"] - open_close_entry["open"]) / open_close_entry["open"]
            printer(f"Found abnormal open/close price diff {diff} at\n{open_close_entry}")
            issues_found = True
    else:
        printer("No open/close price extreme diff anomalies found")


    if between_high_diff and between_low_diff:
        grouped = price_df.groupby(pair_id_column)

        #: Record pairs that have anomalies for logging
        anomalies = []

        between_anomaly_count = 0
        for pair, group in grouped:
            for column in ("open", "close"):
                between_anomalies = examine_price_between_time_anomalies(
                    group,
                    high_diff=between_high_diff,
                    low_diff=between_low_diff,
                    column=column,
                )

                between_anomaly_count += len(between_anomalies)

                # Only record last one per pair
                if len(between_anomalies) > 0:
                    row = between_anomalies.iloc[-1]
                    anomalies.append((pair, row))

        if anomalies:
            print(f"Found {between_anomaly_count} price entries that greatly differ from one before and after")
            for entry in anomalies[0:max_print]:
                pair, diagnostics_row = entry
                timestamp = diagnostics_row["timestamp"]
                printer(f"Found abnormal between price on {pair} at {timestamp}:\n{diagnostics_row}")
            issues_found = True
        else:
            printer("No price time frame in-between extreme diff anomalies found")

    if not issues_found:
        printer(f"No data issues found, {len(price_df)} rows analysed")

    return issues_found


def normalise_volume(df: pd.DataFrame) -> pd.DataFrame:
    """Clean volume data columns.

    - We have different ways to track volume depending on the DEX type

    - The underlying nuances should not matter

    - Normalise volume across all DEXes

    - Run before :py:func:`fix_dex_price_data`

    - The root cause is that uniswap_v2 tracks buy and sell volume, whileas for uniswap v3 we track only volume in the source data

    :return:
        DataFrame where column "volume" is properly filled for all different DEXes.
    """

    assert isinstance(df, pd.DataFrame)

    logger.info("Normalising volume for OHCLV candles data, %d rows", len(df))

    columns = df.columns
    assert "buy_volume" in columns, f"Assume we have buy_volume and sell_volume, we got {columns}. Did we already normalise or did forward fill too early?"

    #             timestamp         open         high          low        close        volume  exchange_rate  buys  sells  buy_volume  sell_volume  pair_id  start_block  end_block  avg
    # timestamp
    # 2023-01-01 2023-01-01  1195.653680  1204.774808  1189.809599  1199.845590  8.087386e+07            1.0   NaN    NaN         NaN          NaN  2697765     16308193   16315354  NaN
    # 2023-01-02 2023-01-02  1199.845590  1230.833520  1193.861634  1213.722761  1.374795e+08            1.0   NaN    NaN         NaN          NaN  2697765     16315362   16322532  NaN
    df["summed_volume"] = df["buy_volume"] + df["sell_volume"]

    # Create a mask when volume column is not set
    #
    mask = df['volume'] == 0 | df['volume'].isna()

    # Replace for "summed_volume" data if volume column was not earlier set
    df["volume"] = df["volume"].where(
        cond=~mask,  # if cond = false, copy from `other`
        other=df["summed_volume"],
        axis="index"
    )
    return df
