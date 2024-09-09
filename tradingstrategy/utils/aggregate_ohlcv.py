"""Aggregated candles.

- Create aggregated price OHLCV and liquidity candles across all available DEX trading pairs

- See :py:func:`aggregate_ohlcv` for usage

"""
import functools
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from typing import TypeAlias

import ipdb
import pandas as pd
import numpy as np
from pandas.core.groupby import DataFrameGroupBy
from pandas.core.interchange.dataframe_protocol import DataFrame

from tradingstrategy.pair import DEXPair, PandasPairUniverse

#: (chain id)-(base token symbol)-(quote token symbol)-(base token address)-(quote token address)
AggregateId: TypeAlias = str


@dataclass(frozen=True, slots=True)
class AggregateId:

    chain_id: int
    base_token_symbol: str
    quote_token_symbol: str
    base_token_address: str
    quote_token_address: str

    def __repr__(self):
        return f"{self.chain_id}-{self.base_token_symbol}-{self.quote_token_symbol}-{self.base_token_address}-{self.quote_token_address}"


#: trading pair -> underlying  aggregated pair ids map
AggregateMap: TypeAlias = dict[AggregateId, set[int]]

ReverseAggregateMap: TypeAlias = dict[int, AggregateId]


def make_aggregate_id(pair: DEXPair) -> AggregateId:
    """For each aggregated pair, identify them

    - Add both human readable symbols and addresses to a string slug

    :return:
        (chain id)-(base token symbol)-(quote token symbol)-(base token address)-(quote token address)
    """
    return AggregateId(
        pair.chain_id.value,
        pair.base_token_symbol,
        pair.quote_token_symbol,
        pair.base_token_address,
        pair.quote_token_address
    )


def build_aggregate_map(
    pair_universe: PandasPairUniverse,
) -> tuple[AggregateMap, ReverseAggregateMap]:
    """Generate a dict of pairs that trade the same token.

    - return aggregate_id
    """

    aggregates: dict[AggregateId, set[int]]
    aggregates = defaultdict(set)
    reverse_aggregates: ReverseAggregateMap = {}
    for pair in pair_universe.iterate_pairs():
        agg_id = make_aggregate_id(pair)
        aggregates[agg_id].add(pair.pair_id)
        reverse_aggregates[pair.pair_id] = agg_id
    return aggregates, reverse_aggregates


def build_aggregation_source_dataframe_for_asset(price_df: DataFrameGroupBy, pair_ids: set[int]) -> pd.DataFrame:
    """Get all trading pairs to a single dataframe with a single index."""


def calculate_volume_weighted_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate volume weighted average prices (vwap) on OHLCV.

    - Assume input has multiple entries for each timestamp (index) separated by column "pair_id"

    - open, high, low, close columns are weighted by volume column

    - volume is the sum of all volume by timestamp

    - liquidity, if in the columns, is the sum of all liquidity by timestamp

    .. code-block:: text

                    pair_id  open  high  low  close  volume  liquidity
        timestamp
        2020-01-01        1   100   100  100    100     500         10
        2020-02-02        1   100   100  100    100     500         10
        2020-01-01        2   110   110  110    110     250         20
        2020-02-02        2   110   110  110    110     250         20
        2020-02-02        3   200   200  200    200    1000         30


    :param df:
        Must have MultiIndex (pair, timestamp)
    """

    assert isinstance(df.index, pd.DatetimeIndex)

    timestamp_agg = df.groupby(level='timestamp')

    # Calculate 0..1 weight for each (pair_id, timestamp) combo
    df["total_volume_in_timestamp"] = timestamp_agg.agg("volume").sum()
    df["weight"] = df["volume"] / df["total_volume_in_timestamp"]

    result_df = pd.DataFrame()
    df["open_weighted"] = (df["open"] * df["weight"])
    df["high_weighted"] = (df["high"] * df["weight"])
    df["low_weighted"] = (df["low"] * df["weight"])
    df["close_weighted"] = (df["close"] * df["weight"])

    grouped_2 = df.groupby("timestamp")
    result_df["open"] = grouped_2["open_weighted"].sum()
    result_df["high"] = grouped_2["high_weighted"].sum()
    result_df["low"] = grouped_2["low_weighted"].sum()
    result_df["close"] = grouped_2["close_weighted"].sum()
    result_df["volume"] = grouped_2["volume"].sum()
    result_df["liquidity"] = grouped_2["liquidity"].sum()
    return result_df


def aggregate_ohlcv_across_pairs(
    pair_universe: PandasPairUniverse,
    price_df: pd.DataFrame,
    liquidity_df: DataFrameGroupBy | None = None,
) -> pd.DataFrame:
    """Builds an aggregates dataframe for trading data.

    - Merge all pools e.g. ETH/USDC Uniswap v2, ETH/USDC Uniswap v3 30 BPS and 5 BPS to a single volume data

    - Prices are weighted by volume

    - Currently supports same-chain pairs only

    :param pairs_df:
        Pair metadata

    :param price_df:
        OHLCV dataframe.

        Must be forward filled.

    :param liquidity_df:
        Liquidity dataframe.

        Must be forward filled.

        Only "close" column is used.

    :return:
        DataFrame with following colmuns

        - aggregate_id
        - base
        - quote
        - open
        - low
        - high
        - close
        - volume
        - liquidity
        - pair_ids (list of ints)

        Volume and liquidity are in USD.
    """

    assert isinstance(pair_universe, PandasPairUniverse)
    # assert isinstance(price_df, pd.DataFrame)

    aggregates, reverse_aggregates =  build_aggregate_map(
        pair_universe,
    )

    assert "timestamp" in price_df.obj.columns  # TODO: Generate from index if not present as a column
    price_indexed_pair_df = price_df.obj.droplevel("timestamp")

    price_df_raw = price_df.obj
    if liquidity_df is not None:
        liquidity_df_raw = liquidity_df.obj
    else:
        liquidity_df_raw = None

    # result_df = pd.DataFrame()
    chunks = []

    # Aggregate each asset by its underlying trading pairs
    for agg_id, pair_ids in aggregates.items():
        # Select all candle data where trading pair belongs to this aggregate
        selected_rows = price_df_raw.loc[price_df_raw.index.get_level_values(0).isin(pair_ids)]
        selected_rows["pair_id"] = selected_rows.index.get_level_values(0)

        if liquidity_df_raw is not None:
            # pair_id  timestamp
            # 2697585  2023-07-02    2.654539e+07
            #          2023-07-09    1.086443e+07

            liquidity_rows = liquidity_df_raw.loc[liquidity_df_raw.index.get_level_values(0).isin(pair_ids)]
            selected_rows["liquidity"] = liquidity_rows

        selected_rows = selected_rows.set_index("timestamp")
        aggregated_rows = calculate_volume_weighted_ohlc(selected_rows)
        aggregated_rows["aggregate_id"] = str(agg_id)
        aggregated_rows["base"] = agg_id.base_token_symbol
        aggregated_rows["quote"] = agg_id.quote_token_symbol

        # https://stackoverflow.com/a/71977912/315168
        q = np.array([1,], dtype=object)   # dummy array, note the dtype
        q[0] = list(pair_ids)                      # squeeze the list into single cell
        aggregated_rows["pair_ids"] = np.tile(q, aggregated_rows.shape[0])  # tile and assign

        chunks.append(aggregated_rows)

    return pd.concat(chunks)
