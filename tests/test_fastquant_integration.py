"""Fastquant integration smoke tests."""

import datetime

import backtrader as bt
import pytest
from IPython.core.display import display
from backtrader import analyzers
import pandas as pd
from fastquant import backtest

from tradingstrategy.candle import GroupedCandleUniverse
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.frameworks.backtrader import prepare_candles_for_backtrader, add_dataframes_as_feeds, DEXFeed
from tradingstrategy.frameworks.fastquant import to_human_readable_result
from tradingstrategy.pair import PandasPairUniverse



def test_fastquant_smac(logger, persistent_test_client: Client):
    """Run Fasttrader SMAC on a single pair over Capitalgram API."""

    capitalgram = persistent_test_client

    exchange_universe = capitalgram.fetch_exchange_universe()

    # Fetch all trading pairs across all exchanges
    columnar_pair_table = capitalgram.fetch_pair_universe()
    pair_universe = PandasPairUniverse(columnar_pair_table.to_pandas())

    # Pick SUSHI-USDT trading on SushiSwap
    sushi_swap = exchange_universe.get_by_name_and_chain(ChainId.ethereum, "sushiswap")
    sushi_usdt = pair_universe.get_one_pair_from_pandas_universe(
        sushi_swap.exchange_id,
        "SUSHI",
        "USDT")

    # Get daily candles as Pandas DataFrame
    all_candles = capitalgram.fetch_all_candles(TimeBucket.d1).to_pandas()
    sushi_usdt_candles: pd.DataFrame  = all_candles.loc[all_candles["pair_id"] == sushi_usdt.pair_id]

    # Reformat data suitable for Backtrader based backtesting
    sushi_usdt_candles = prepare_candles_for_backtrader(sushi_usdt_candles)

    # To make this notebook deterministic, we pick date ranges for the test
    start = datetime.datetime(2020, 9, 15)
    end = datetime.datetime(2021, 7, 1)

    sushi_usdt_candles = sushi_usdt_candles[
        (sushi_usdt_candles.index >= start) &
        (sushi_usdt_candles.index <= end)]

    print(f"We have {len(sushi_usdt_candles)} daily candles for SUSHI-USDT")

    result, trade_graph = backtest(
        'smac',  # Simple Moving Average Crossover
        sushi_usdt_candles,
        init_cash=10_000, # Start with 10,000 USD
        fast_period=10,
        slow_period=20,
        plot=False,
        return_plot=True,
        verbose=0)

    display(to_human_readable_result(result))



