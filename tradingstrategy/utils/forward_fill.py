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
import logging
import warnings

from typing import Collection

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy


logger = logging.getLogger(__name__)


def generate_future_filler_data(
    last_valid_row: pd.Series,
    timestamp: pd.Timestamp,
    columns: Collection[str],
):
    """Create a new placeholder OHLCV entry based on the last valid entry."""
    new_row = {}
    last_close = last_valid_row["close"]

    for col in columns:
        match col:
            case "open" | "high" | "low" | "close":
                new_row[col] = last_close
            case "volume":
                new_row[col] = 0
            case "timestamp":
                new_row[col] = timestamp
            case _:
                raise NotImplementedError(f"Unsupported column {col}")

    return new_row


def fill_future_gap(
    df,
    timestamp: pd.Timestamp,
    columns: Collection[str],
    pair_hint: str | None = None,
):
    """Add a virtual OHLCV value at the end of the pair OHLCV data series if there is no real value."""

    assert isinstance(df, pd.DataFrame)
    assert isinstance(df.index, pd.DatetimeIndex), f"Expected DatetimeIndex index, got {type(df.index)}"

    if timestamp not in df.index:
        # Get the latest valid entry before the timestamp
        last_valid_ts = df.index[-1]
        last_valid_entry = df.loc[last_valid_ts]
        data = generate_future_filler_data(
            last_valid_entry, timestamp, columns
        )
        # Create a new row with the timestamp and the last valid entry's values]
        df.loc[timestamp] = data

        logger.debug(
            "Pair %s: Added data end marker at %s, last entry was %s",
            pair_hint,
            timestamp,
            last_valid_ts
        )
    else:
        logger.debug("Pair %s: no need to add data end marker", pair_hint)

    return df


def fill_future_gap_multi_pair(
    grouped_df,
    timestamp: pd.Timestamp,
    columns: Collection[str],
):
    assert isinstance(grouped_df, DataFrameGroupBy)

    def _apply(df):
        df = fill_future_gap(
            df,
            timestamp,
            columns,
            pair_hint=df.name,
        )
        return df

    fixed = grouped_df.apply(_apply)

    # Annoying: sometimes we may have timestamp column, sometimes we don't
    # depends on the caller and is not normalised
    if "timestamp" in fixed.columns:
        del fixed["timestamp"]

    return fixed.reset_index().set_index("timestamp").groupby("pair_id")


def forward_fill(
    single_or_multipair_data: pd.DataFrame | DataFrameGroupBy,
    freq: pd.DateOffset | str,
    columns: Collection[str] = ("open", "high", "low", "close", "volume", "timestamp"),
    drop_other_columns = True,
    forward_fill_until: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Forward-fill OHLCV data for multiple trading pairs.

    :py:term:`Forward fill` certain candle columns.

    If multiple pairs are given as a `GroupBy`, then the data is filled
    only for the min(pair_timestamp), max(timestamp) - not for the
    range of the all data.

    .. note ::

        `timestamp` and `pair_id` columns will be deleted in this process
         - do not use these columns, but corresponding indexes instead.

    See also

    - :py:func:`tradingstrategy.utils.groupeduniverse.resample_candles`

    - :py:func:`tradingstrategy.utils.groupeduniverse.resample_series`

    Example:

    .. code-block:: python

        import os

        from tradingstrategy.chain import ChainId
        from tradingstrategy.client import Client
        from tradingstrategy.timebucket import TimeBucket
        from tradingstrategy.utils.forward_fill import forward_fill
        from tradingstrategy.utils.groupeduniverse import fix_bad_wicks

        from tradeexecutor.strategy.execution_context import python_script_execution_context
        from tradeexecutor.strategy.trading_strategy_universe import load_all_data
        from tradeexecutor.strategy.universe_model import UniverseOptions

        client = Client.create_jupyter_client()

        chain_id = ChainId.polygon
        time_bucket = TimeBucket.d1
        exchange_slug = "uniswap-v3"

        exchanges = client.fetch_exchange_universe()
        uni = exchanges.get_by_chain_and_slug(ChainId.polygon, exchange_slug)

        dataset = load_all_data(
            client,
            time_frame=TimeBucket.d1,
            execution_context=python_script_execution_context,
            universe_options=UniverseOptions(),
            with_liquidity=False,
        )

        # Filter out pair ids that belong to our target dataset
        pair_universe = dataset.pairs
        pair_ids = pair_universe.loc[pair_universe["exchange_id"] == uni.exchange_id]["pair_id"]
        filtered_df = dataset.candles.loc[dataset.candles["pair_id"].isin(pair_ids)]

        # Forward fill data
        filtered_df = filtered_df.set_index("timestamp")

        # Sanitise price data
        filtered_df = fix_bad_wicks(filtered_df)

        # Make sure there are no gaps in the data
        filtered_df = filtered_df.groupby("pair_id")
        pairs_df = forward_fill(
            filtered_df,
            freq=time_bucket.to_frequency(),
            columns=("open", "high", "low", "close", "volume"),
        )

        # Wrote Parquest file under /tmp
        fpath = f"/tmp/{chain_id.get_slug()}-{exchange_slug}-candles-{time_bucket.value}.parquet"
        flattened_df = pairs_df.obj
        flattened_df = flattened_df.reset_index().set_index("timestamp")  # Get rid of grouping
        flattened_df.to_parquet(fpath)
        print(f"Wrote {fpath} {os.path.getsize(fpath):,} bytes")

    :param single_or_multipair_data:
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

        To get all OHLC data set this to `("open", "high", "low", "close")`.

        If the data has `timestamp` column we fill it with the first value.

    :param drop_other_columns:
        Remove other columns before forward-fill to save memory.

        The resulting DataFrame will only have columns listed in `columns`
        parameter.

        The removed columns include ones like `high` and `low`, but also Trading Strategy specific
        columns like `start_block` and `end_block`. It's unlikely we are going to need
        forward-filled data in these columns.

        .. note ::

            We have no logic for forward filling random columns, only mentioned columns.

    :param forward_fill_until:
        The timestamp which we know the data is valid for.

        If there are price gaps at rarely traded pairs at the end of the (live) OHLCV series,
        we will forward fill the data until this timestamp.

        If not given forward fills until the last trade of the pair.

        The timestamp must match the index timestamp frequency      .

    :return:
        DataFrame where each timestamp has a value set for columns.

        For multi pair data if input is `DataFrameGroupBy` then a similar `DataFrameGroupBy` is
        returned.
    """

    assert isinstance(single_or_multipair_data, (pd.DataFrame, DataFrameGroupBy))
    assert isinstance(freq, (pd.DateOffset, str)), f"Expected pd.DateOffset, got: {freq}"

    original = single_or_multipair_data
    grouped = isinstance(single_or_multipair_data, DataFrameGroupBy)

    # https://www.statology.org/pandas-drop-all-columns-except/
    if drop_other_columns:
        single_or_multipair_data = single_or_multipair_data[list(columns)]

    # Set the end marker if we know when the data should end
    if forward_fill_until is not None:
        assert isinstance(forward_fill_until, pd.Timestamp), f"Got: {type(forward_fill_until)}"

        if grouped:
            single_or_multipair_data = fill_future_gap_multi_pair(single_or_multipair_data, forward_fill_until, columns)
        else:
            single_or_multipair_data = fill_future_gap(single_or_multipair_data, forward_fill_until, columns)

    # Fill missing timestamps with NaN
    # https://stackoverflow.com/a/45620300/315168
    # This will also ungroup the data
    with warnings.catch_warnings():
        # FutureWarning: https://stackoverflow.com/questions/77969964/deprecation-warning-with-groupby-apply
        warnings.simplefilter("ignore")
        single_or_multipair_data = single_or_multipair_data.resample(freq).mean(numeric_only=True)

    if grouped:
        # resample() will set pair_id to NaN
        # fix here
        single_or_multipair_data["pair_id"] = single_or_multipair_data.index.get_level_values('pair_id')

    columns = set(columns)

    # Force columns to be forward filled in a certain order
    # We always need to ffill close column first
    for column in ("close", "open", "high", "low", "volume", "timestamp"):
        if column in columns:
            columns.remove(column)

            match column:
                case "volume":
                    # Sparse volume is 0
                    single_or_multipair_data["volume"] = single_or_multipair_data["volume"].fillna(0.0)
                case "close":
                    # Sparse close is the previous close
                    single_or_multipair_data["close"] = single_or_multipair_data["close"].ffill()
                case "open" | "high" | "low":
                    # Fill open, high, low from the ffill'ed close.
                    single_or_multipair_data[column] = single_or_multipair_data[column].fillna(single_or_multipair_data["close"])
                case "timestamp":

                    if grouped:
                        check_columns = original.obj.columns
                    else:
                        check_columns = original.columns

                    if isinstance(single_or_multipair_data.index, pd.MultiIndex):
                        if "timestamp" in check_columns:
                            # pair_id, timestamp index
                            single_or_multipair_data["timestamp"] = single_or_multipair_data.index.get_level_values(1)
                    elif isinstance(single_or_multipair_data.index, pd.DatetimeIndex):
                        if "timestamp" in check_columns:
                            # timestamp index
                            single_or_multipair_data["timestamp"] = single_or_multipair_data.index
                    else:
                        raise NotImplementedError(f"Unknown column: {column} - forward_fill() does not know how to handle")

    if columns:
        # Unprocessable columns left
        raise NotImplementedError(f"Does not know how to forward fill: {columns}")

    # Regroup by pair, as this was the original data format
    if grouped:
        single_or_multipair_data["timestamp"] = single_or_multipair_data.index.get_level_values('timestamp')
        single_or_multipair_data = single_or_multipair_data.groupby(level="pair_id")

    return single_or_multipair_data
