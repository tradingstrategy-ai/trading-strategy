"""Backtrader integration smoke test.

Also some examples how to extract meaningful data out from the backtrader outputs."""

import datetime

import backtrader as bt
import pytest
from backtrader import analyzers
from backtrader import indicators
import pandas as pd

from tradingstrategy.candle import GroupedCandleUniverse
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.frameworks.backtrader import prepare_candles_for_backtrader, add_dataframes_as_feeds, CapitalgramFeed
from tradingstrategy.pair import PandasPairUniverse


# From https://www.backtrader.com/home/helloalgotrading/
class SmaCross(bt.Strategy):

    # The strategy parameters do not make any sense,
    # they are just random numbers to get some test output
    params = dict(
        pfast=5,  # period for the fast moving average
        pslow=10   # period for the slow moving average
    )

    def __init__(self):
        sma1 = indicators.SMA(period=self.p.pfast)  # fast moving average
        sma2 = indicators.SMA(period=self.p.pslow)  # slow moving average
        self.crossover = bt.ind.CrossOver(sma1, sma2)  # crossover signal
        self.ticks = 0

    def next(self):

        self.ticks += 1

        if not self.position:  # not in the market
            if self.crossover > 0:  # if fast crosses slow to the upside
                self.buy()  # enter long

        elif self.crossover < 0:  # in the market & cross to the downside
            self.close()  # close long position


class IntegrationTestStrategy(bt.Strategy):
    """Not a real strategy - just does some asserts"""

    def __init__(self):
        self.ticks = 0
        first_feed = self.datas[0]
        assert isinstance(first_feed, CapitalgramFeed)
        assert first_feed.pair_info.base_token_symbol == "WETH"
        assert first_feed.pair_info.quote_token_symbol == "USDT"

    def next(self):
        self.ticks += 1


def test_backtrader_sma(logger, persistent_test_client: Client):
    """Run Backtrader SMA on a single pair over Capitalgram API."""

    client = persistent_test_client

    # exchange_universe = client.fetch_exchange_universe()
    raw_pairs = client.fetch_pair_universe().to_pandas()

    # Use daily candles in this test as BT assumes daily
    raw_candles = client.fetch_all_candles(TimeBucket.d1).to_pandas()

    pair_universe = PandasPairUniverse(raw_pairs)
    candle_universe = GroupedCandleUniverse(raw_candles)

    # Do some test calculations for a single pair
    # sushi_swap = exchange_universe.get_by_name_and_chain(ChainId.ethereum, "sushiswap")
    sushi_swap_exchange_id = 22  # Test Speed up
    sushi_usdt = pair_universe.get_one_pair_from_pandas_universe(sushi_swap_exchange_id, "SUSHI", "USDT")

    sushi_usdt_candles = candle_universe.get_candles_by_pair(sushi_usdt.pair_id)
    sushi_usdt_candles = prepare_candles_for_backtrader(sushi_usdt_candles)

    # Make the test derministic by using a known date range
    start_date = datetime.datetime(2020, 10, 1)
    end_date = datetime.datetime(2021, 5, 1)
    sushi_usdt_candles = sushi_usdt_candles[start_date:end_date]
    assert len(sushi_usdt_candles) > 90  # Check we did not screw up with the data

    cerebro = bt.Cerebro(stdstats=False, maxcpus=1, optreturn=False)
    cerebro.addobserver(bt.observers.Broker)
    cerebro.addobserver(bt.observers.Trades)
    cerebro.addobserver(bt.observers.BuySell)

    cerebro.addanalyzer(analyzers.Returns, _name="returns")
    cerebro.addanalyzer(analyzers.SharpeRatio, _name="mysharpe")
    cerebro.addanalyzer(analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(analyzers.TimeDrawDown, _name="timedraw")
    cerebro.addanalyzer(analyzers.TradeAnalyzer, _name="tradeanalyzer")  # trade analyzer

    backtrader_feed = bt.feeds.PandasData(dataname=sushi_usdt_candles)
    cerebro.adddata(backtrader_feed)
    cerebro.addstrategy(SmaCross)

    # Print out the starting conditions
    logger.info('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    assert cerebro.broker.getvalue() == 10000.00
    results = cerebro.run()
    logger.info('Ending Portfolio Value: %.2f' % cerebro.broker.getvalue())
    assert cerebro.broker.getvalue() == pytest.approx(10009.08)

    sma_cross: SmaCross = results[0]

    # We run the strategy over 202 days
    assert sma_cross.ticks == 203

    # Check some analyzer results

    # Strategy returns
    returns: analyzers.Returns = sma_cross.analyzers.returns
    assert returns.rets["rtot"] == pytest.approx(0.0009072160270230083)

    # Won/loss trades
    trade_analyzer: analyzers.TradeAnalyzer = sma_cross.analyzers.tradeanalyzer
    assert trade_analyzer.rets["won"]["total"] == 4
    assert trade_analyzer.rets["lost"]["total"] == 5


def test_backtrader_multiasset(logger, persistent_test_client: Client):
    """Mutliasset strategy runs correct number of days."""

    client = persistent_test_client

    # Decompress the pair dataset to Python map
    columnar_pair_table = client.fetch_pair_universe()

    # Convert PyArrow table to Pandas format to continue working on it
    all_pairs_dataframe = columnar_pair_table.to_pandas()

    # Filter down to pairs that only trade on Sushiswap
    sushi_swap_exchange_id = 22  # Test Speed up
    sushi_pairs: pd.DataFrame = all_pairs_dataframe.loc[
        (all_pairs_dataframe['exchange_id'] == sushi_swap_exchange_id) &  # Trades on Sushi
        (all_pairs_dataframe['buy_volume_all_time'] > 500_000)  # 500k min buys
    ]
    # Create a Python set of pair ids
    wanted_pair_ids = sushi_pairs["pair_id"]

    # Make the trading pair data easily accessible
    pair_universe = PandasPairUniverse(sushi_pairs)

    print(f"Sushiswap on Ethereum has {len(pair_universe.get_all_pair_ids())} legit trading pairs")

    # Get daily candles as Pandas DataFrame
    all_candles = client.fetch_all_candles(TimeBucket.d1).to_pandas()
    sushi_candles: pd.DataFrame = all_candles.loc[all_candles["pair_id"].isin(wanted_pair_ids)]

    sushi_candles = prepare_candles_for_backtrader(sushi_candles)

    # We limit candles to a specific date range to make this notebook deterministic
    start = datetime.datetime(2020, 10, 1)
    end = datetime.datetime(2021, 6, 1)

    sushi_candles = sushi_candles[(sushi_candles.index >= start) & (sushi_candles.index <= end)]

    # Group candles by the trading pair ticker
    sushi_tickers = GroupedCandleUniverse(sushi_candles)

    print(f"Out candle universe size is {len(sushi_candles)}")

    # Create a cerebro entity
    cerebro = bt.Cerebro(stdstats=False)

    # Add a strategy
    cerebro.addstrategy(IntegrationTestStrategy)

    # Pass all Sushi pairs to the data fees to the strategy
    # noinspection JupyterKernel
    feeds = [df for pair_id, df in sushi_tickers.get_all_pairs()]
    add_dataframes_as_feeds(
        cerebro,
        pair_universe,
        feeds,
        start,
        end,
        TimeBucket.d1)

    results = cerebro.run()

    strategy: IntegrationTestStrategy = results[0]

    # We run the strategy over 202 days
    assert strategy.ticks == 244



