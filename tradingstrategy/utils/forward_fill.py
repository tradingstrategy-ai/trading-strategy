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
from typing import Tuple, Collection

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy


def forward_fill(
    df: pd.DataFrame | DataFrameGroupBy,
    freq: pd.DateOffset,
    columns: Collection[str] = ("open", "close"),
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

        To get all OHLC data set this to ``("open", "high", "low", "close")``.

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
    for column in ("close", "open", "high", "low", "volume"):
        if column in columns:
            columns.remove(column)

            match column:
                case "volume":
                    # Sparse volume is 0
                    df["volume"] = df["volume"].fillna(0.0)
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
