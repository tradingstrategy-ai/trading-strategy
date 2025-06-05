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
import datetime
import logging
import warnings

from typing import Collection

import pandas as pd
from pandas._libs.tslibs import BaseOffset
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

    new_row["forward_filled"] = True
    return new_row


def fill_future_gap(
    df,
    timestamp: pd.Timestamp,
    columns: Collection[str],
    pair_hint: str | None = None,
):
    """Add a virtual OHLCV value at the end of the pair OHLCV data series if there is no real value.

    :param pair_hint:
        A hint for the pair name, used in errors and logging, if something goes wrong.
    """

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
    forward_fill_until: pd.Timestamp | datetime.datetime | None = None,
) -> pd.DataFrame | DataFrameGroupBy:
    """Forward-fill OHLCV data for single or multiple trading pairs.

    :py:term:`Forward fill` certain candle columns. Forward

    If multiple pairs are given as a `GroupBy`, then the data is filled
    only for the min(pair_timestamp), max(timestamp) - not for the
    range of the all data.

    When ``forward_fill_until`` is given, Forward filled OHCLV data values "forward_filled" column set to True.
    ``forward_filled`` column is set on all pairs - if any of pairs have any forward filled
    values in that timestamp index.

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

    if isinstance(forward_fill_until, datetime.datetime):
        forward_fill_until = pd.Timestamp(forward_fill_until)

    if isinstance(single_or_multipair_data, DataFrameGroupBy):
        df = single_or_multipair_data.obj
        grouped = True
    else:
        grouped = False
        df = single_or_multipair_data

    if grouped:
        # We do multiple pairs
        df = resample_candles_multiple_pairs(
            df,
            frequency=freq,
            forward_fill_until=forward_fill_until,
            forward_fill_columns=columns,
        )
    else:
        # Data is only for a single pair
        df = forward_fill_ohlcv_single_pair(
            df,
            freq=freq,
            forward_fill_until=forward_fill_until,
        )

    # Regroup by pair, as this was the original data format
    if grouped:

        # Not really needed, but some legacy code depends on this.
        # We will index the underlying DataFrame by MultiIndex(pair_id, timestamp)
        # and then create groupby by pair_id.
        #if "timestamp" not in df.columns and isinstance(df.index, pd.DatetimeIndex):
            # If we have timestamp index, then we can use it
        # df["timestamp"] = df.index

        if df.index.name == "timestamp" and "timestamp" in df.columns:
            df = df.reset_index(drop=True)
        else:
            df = df.reset_index()
        df = df.sort_values(by=["pair_id", "timestamp"])
        df = df.set_index(["pair_id", "timestamp"], drop=False)
        dfgb = df.groupby(level="pair_id")
        return dfgb
    else:
        return df


def xxx_forward_fill(
    single_or_multipair_data: pd.DataFrame | DataFrameGroupBy,
    freq: pd.DateOffset | str,
    columns: Collection[str] = ("open", "high", "low", "close", "volume", "timestamp"),
    drop_other_columns = True,
    forward_fill_until: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Forward-fill OHLCV data for multiple trading pairs.

    :py:term:`Forward fill` certain candle columns. Forward

    If multiple pairs are given as a `GroupBy`, then the data is filled
    only for the min(pair_timestamp), max(timestamp) - not for the
    range of the all data.

    When ``forward_fill_until`` is given, Forward filled OHCLV data values "forward_filled" column set to True.
    ``forward_filled`` column is set on all pairs - if any of pairs have any forward filled
    values in that timestamp index.

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
            raw_single_or_multipair_data = single_or_multipair_data
            single_or_multipair_data = fill_future_gap_multi_pair(single_or_multipair_data, forward_fill_until, columns)
            original_index = single_or_multipair_data.obj.index
            new_index = single_or_multipair_data.obj.index
            ff_df = single_or_multipair_data.obj
        else:
            raw_single_or_multipair_data = single_or_multipair_data
            original_index = single_or_multipair_data.index
            single_or_multipair_data = fill_future_gap(single_or_multipair_data, forward_fill_until, columns)
            new_index = single_or_multipair_data.index
            ff_df = single_or_multipair_data

        single_or_multipair_data = ff_df

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


def resample_candles_multiple_pairs(
    df: pd.DataFrame,
    frequency: str,
    pair_id_column="pair_id",
    copy_columns=["pair_id"],
    forward_fill_columns: Collection[str]=("open", "high", "low", "close", "volume",),
    fix_and_sort_index=True,
    forward_fill_until: datetime.datetime | None = None,
    multipair: bool = True,
) -> pd.DataFrame:
    """Upsample a OHLCV trading pair data to a lower time bucket.

    - First group the DataFrame by pair
    - Transform
    - Resample in OHLCV manner
    - Forward fill any gaps in data
    - Set `forward_fill_until` attribute on DataFrame to refect how much forward fill was done

    :param pair_id_column:
        DataFrame column to group the data by pair

    :param copy_columns:
        Columns we simply copy over.

        We assume every pair has the same value for these columns.

    :parma fix_and_sort_index:
        Make sure we have a good timestamp index before proceeding.

    :return:
        Concatenated DataFrame of individually resampled pair data
    """

    if fix_and_sort_index:
        # Make sure timestamp is used as index, not as a column, beyond this point
        if not isinstance(df.index, pd.DatetimeIndex) and "timestamp" in df.columns:
            df = df.set_index("timestamp")
            df = df.sort_index()


    assert pair_id_column in df.columns, f"Trying to break multipair data to individual pairs, but {pair_id_column} is not in the DataFrame columns {df.columns.tolist()}."

    by_pair = df.groupby(pair_id_column)
    segments = []

    # Resample each pair separately
    for group_id in by_pair.groups:
        pair_df = by_pair.get_group(group_id)

        # Does this pair have any data to resample?
        if len(pair_df) > 0:
            segment = resample_candles(pair_df, frequency)

            # Fill pair_id for all rows
            for c in copy_columns:
                if c in pair_df.columns:
                    first_row = pair_df.iloc[0]
                    segment[c] = first_row[c]

            if forward_fill_until is not None:
                segment = forward_fill_ohlcv_single_pair(
                    segment,
                    freq=frequency,
                    forward_fill_until= forward_fill_until,
                    pair_id=group_id,
                )
            else:
                # Forward fill OHLCV if we went from 1d -> 1h
                for ff_column in forward_fill_columns:
                    segment[ff_column] = segment[ff_column].ffill()

            segments.append(segment)

    df = pd.concat(segments)
    df.attrs["forward_filled_until"] = forward_fill_until
    return df


def resample_candles(
    df: pd.DataFrame,
    resample_freq: pd.Timedelta | BaseOffset | str,
    shift: int | None=None,
    origin: str | None=None,
) -> pd.DataFrame:
    """Downsample or upsample OHLCV candles or liquidity samples.

    E.g. upsample 1h candles to 1d candles.

    Limited to one pair per ``DataFrame``. See also: py:func:`resample_price_series`
    and
    :py:func:`tradeexecutor.strategy.pandas_trader.alternative_market_data.resample_multi_pair`
    for resamping multipair data.

    Example:

    .. code-block:: python

        # Transform daily candles to monthly candles
        from tradingstrategy.utils.groupeduniverse import resample_candles

        single_pair_candles = raw_candles.loc[raw_candles["pair_id"] == pair.pair_id]
        single_pair_candles = single_pair_candles.set_index("timestamp", drop=False)
        monthly_candles = resample_candles(single_pair_candles, TimeBucket.d30)
        monthly_candles = resample_candles(single_pair_candles, TimeBucket.d30)
        assert len(monthly_candles) <= len(single_pair_candles) / 4

    :param df:
        DataFrame of price, liquidity or lending rate candles.

        Must contain candles only for a single trading pair.

        Supported columns: open, high, low, close.
        Optional: pair_id, volume.

        Any other columns in DataFrame are destroyed in the resampling process.

    :param resample_freq:
        Resample frequency.

        Timedelta or Pandas alias string e.g. "D".

        E.g.`pd.Timedelta(days=1)` create daily candles from hourly candles.

    :param shift:
        Before resampling, shift candles to left or right.

        The shift is measured in number of candles, not time.
        Make sure the DataFrame is forward filled first,
        see :py:func:`forward_fill`.

        Set to `1` to shift candles one step right,
        `-1` to shift candles one step left.

        There might not be enough rows to shift. E.g. shift=-1 or shift=1 and len(df) == 1.
        In this case, an empty data frame is returned.

    :param origin:
        For daily resample, the starting hour.

        Use `origin="end"` for a rolling resample.

    :return:
        Resampled candles in a new DataFrame.

        Contains an added `timestamp` column that is also the index.

        If the input DataFrame is zero-length, then return it as is.

    """

    if not type(resample_freq) == str:
        if isinstance(resample_freq, BaseOffset):
            resample_freq =  pd.Timedelta(nanoseconds=resample_freq.nanos)
        assert isinstance(resample_freq, pd.Timedelta), f"We got {resample_freq}, supposed to be pd.Timedelta. E.g. pd.Timedelta(hours=2)"

    if len(df) == 0:
        return df

    # Sanity check we don't try to resample mixed data of multiple pairs
    if "pair_id" in df.columns:
        pair_ids = df["pair_id"].unique()
        assert len(pair_ids) == 1, f"resample_candles() can do only a single pair. Data must have single pair_id only. We got {len(pair_ids)} pair ids: {pair_ids}, columns: {df.columns}"
        pair_id = pair_ids[0]
    else:
        pair_id = None

    ohlc_dict = {}

    if "open" in df.columns:
        ohlc_dict["open"] = "first"

    if "high" in df.columns:
        ohlc_dict["high"] = "max"

    if "low" in df.columns:
        ohlc_dict["low"] = "min"

    if "close" in df.columns:
        ohlc_dict["close"] = "last"

    if "volume" in df.columns:
        ohlc_dict["volume"] = "sum"

    columns = df.columns.tolist()
    assert all(item in columns for item in list(ohlc_dict.keys())), \
        f"{list(ohlc_dict.keys())} needs to be in the column names\n" \
        f"We got columns: {df.columns.tolist()}"

    if shift:
        df = df.shift(shift).dropna()

    if origin:
        candles = df.resample(resample_freq, origin=origin).agg(ohlc_dict)
    else:
        # https://stackoverflow.com/questions/21140630/resampling-trade-data-into-ohlcv-with-pandas
        candles = df.resample(resample_freq).agg(ohlc_dict)

    # TODO: Figure out right way to preserve timestamp column,
    # resample seems to destroy it
    candles["timestamp"] = candles.index

    if pair_id:
        candles["pair_id"] = pair_id

    return candles



def forward_fill_ohlcv_single_pair(
    df: pd.DataFrame,
    freq: pd.DateOffset | str,
    forward_fill_until: pd.Timestamp | datetime.datetime,
    pair_id: int | None = None,
) -> pd.DataFrame:
    """Forward-fill OHLCV data in a Pandas DataFrame, ensuring OHCLV logical consistency.

    This function forward-fills missing OHLCV (Open, High, Low, Close, Volume) data
    in a Pandas DataFrame while maintaining logical rules:
    - High, Low, and Open are set to the last valid Close value.
    - Volume is set to 0.
    - A boolean column 'forward_filled' is added to indicate which rows were forward-filled.

    :param df:
        The Pandas DataFrame containing OHLCV data.
        The DataFrame must have a DatetimeIndex.
    :param freq:
        The frequency to resample the data to (e.g., '1H' for hourly).
    :param columns:
        A list of columns to forward-fill. Must include 'open', 'high', 'low', 'close', and 'volume'.
    :param forward_fill_until:
        An optional timestamp to limit the forward-fill to.
    :param pair_id:
        Fill in pair_id column with this value.

    :return:
        A new Pandas DataFrame with forward-filled OHLCV data and a 'forward_filled' column.
    """

    assert isinstance(df.index, pd.DatetimeIndex), "DataFrame must have a DatetimeIndex."

    if forward_fill_until is not None:
        forward_fill_until = pd.Timestamp(forward_fill_until)

    # Check if the DataFrame is empty
    df["forward_filled"] = False

    # Resample
    original_index = df.index
    df = df.resample(freq).mean(numeric_only=True)

    df["forward_filled"] = df["forward_filled"].fillna(True)

    if forward_fill_until is not None:
        df = pad_dataframe_to_frequency(
            df,
            freq,
            end_timestamp=forward_fill_until,
        )

    # Forward fill 'close' values
    if "close" in df.columns:
        df["close"] = df["close"].ffill()

    # Set 'open', 'high', and 'low' to the forward-filled 'close' value
    if "open" in df.columns:
        df["open"] = df["open"].fillna(df["close"])

    if "high" in df.columns:
        df["high"] = df["high"].fillna(df["close"])

    if "low" in df.columns:
        df["low"] = df["low"].fillna(df["close"])

    # Set 'volume' to 0
    if "volume" in df.columns:
        # Volume not present in TVL OHLC data
        df["volume"] = df["volume"].fillna(0)

    # Mark the rows that were forward-filled
    new_index = df.index
    forward_filled_indices = new_index.difference(original_index)

    df["timestamp"] = df.index

    if "pair_id" in df.columns:
        if not pair_id:
            pair_ids = df["pair_id"].dropna().unique()
            assert len(pair_ids) == 1, f"Expected single pair_id, got {len(pair_ids)}: {pair_ids}"
            pair_id = pair_ids[0]

        assert pair_id
        df["pair_id"] = pair_id

    return df



def pad_dataframe_to_frequency(
    df: pd.DataFrame,
    freq: pd.DateOffset | str,
    end_timestamp: pd.Timestamp,
) -> pd.DataFrame:
    """
    Pads a Pandas DataFrame with NaN entries for all columns up to a given end timestamp,
    using the inferred frequency of the DataFrame.

    - Also sets `forward_filled` column to True/False for generated rows.

    :param df:
        The Pandas DataFrame to pad. Must have a DatetimeIndex.
    :param freq:
        The frequency to pad with.
    :param end_timestamp:
        The timestamp to pad the DataFrame up to.

    :return:
        A new Pandas DataFrame with NaN entries added to the end,
        such that the DataFrame extends to the end_timestamp with the given frequency.
    """

    assert isinstance(df.index, pd.DatetimeIndex), "DataFrame must have a DatetimeIndex."
    assert isinstance(end_timestamp, pd.Timestamp), "end_timestamp must be a Pandas Timestamp."

    if len(df) == 0:
        return df

    # Generate a date range from the last timestamp in the DataFrame to the end_timestamp
    # with the specified frequency
    last_timestamp = df.index[-1]
    new_index = pd.date_range(start=last_timestamp, end=end_timestamp, freq=freq)

    if len(new_index) > 0:
        # Filter out the existing index to only get the new dates
        new_index = new_index.difference(df.index)

        # Create a new DataFrame with the new index and NaN values for all columns
        new_df = pd.DataFrame(index=new_index, columns=df.columns).astype(df.dtypes)

        if len(new_df) > 0:
            new_df["forward_filled"] = True  # Cannot have all NA entries            

            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("error")
                    padded_df = pd.concat([df, new_df])
            except Exception as e:
                raise RuntimeError(f"df: {df}\nnew_df: {new_df}\nError: {e}") from e
            return padded_df

    # Nothing to add
    return df