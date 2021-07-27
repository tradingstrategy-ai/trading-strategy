"""Backtrader integration smoke test.

Also some examples how to extract meaningful data out from the backtrader outputs."""

import datetime

import backtrader as bt
import pytest
from backtrader import analyzers
from backtrader import indicators

from capitalgram.candle import CandleBucket, GroupedCandleUniverse
from capitalgram.client import Capitalgram
from capitalgram.frameworks.backtrader import prepare_candles_for_backtrader
from capitalgram.pair import PandasPairUniverse


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


def test_backtrader_sma(logger, persistent_test_client: Capitalgram):
    """Run Backtrader SMA on a single pair over Capitalgram API."""

    client = persistent_test_client

    # exchange_universe = client.fetch_exchange_universe()
    raw_pairs = client.fetch_pair_universe().to_pandas()

    # Use daily candles in this test as BT assumes daily
    raw_candles = client.fetch_all_candles(CandleBucket.h24).to_pandas()

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
    assert sma_cross.ticks == 202

    # Check some analyzer results

    # Strategy returns
    returns: analyzers.Returns = sma_cross.analyzers.returns
    assert returns.rets["ravg"] == pytest.approx(4.279320882184001e-06)
    assert returns.rets["rtot"] == pytest.approx(0.0009072160270230083)

    # Won/loss trades
    trade_analyzer: analyzers.TradeAnalyzer = sma_cross.analyzers.tradeanalyzer
    assert trade_analyzer.rets["won"]["total"] == 4
    assert trade_analyzer.rets["lost"]["total"] == 5
