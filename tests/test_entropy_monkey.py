"""Backtrader integration smoke test.

Also some examples how to extract meaningful data out from the backtrader outputs."""

import datetime
import math
from random import Random

import backtrader as bt
import pandas as pd
from backtrader.indicators import PeriodN
from backtrader import analyzers

from capitalgram.candle import CandleBucket, GroupedCandleUniverse
from capitalgram.client import Capitalgram
from capitalgram.frameworks.backtrader import prepare_candles_for_backtrader, add_dataframes_as_feeds, CapitalgramFeed
from capitalgram.pair import PandasPairUniverse


class PastTradeVolumeIndicator(PeriodN):
    """Indicates whether the trading pair has reached certain volume for the last N days.

    Based on indicator base class that takes period (days) as an input.
    """

    lines = ('cum_volume',)

    def next(self):
        # This indicator is feed with volume line.
        # We simply take the sum of the daily volumes based on the period (number of days)
        datasum = math.fsum(self.data.get(size=self.p.period))
        self.lines.cum_volume[0] = datasum


# https://www.backtrader.com/docu/quickstart/quickstart/#adding-some-logic-to-the-strategy
# https://teddykoker.com/2019/05/momentum-strategy-from-stocks-on-the-move-in-python/
class EntropyMonkey(bt.Strategy):
    """A strategy that picks a new token to go all-in every day."""

    def __init__(self, pair_universe: PandasPairUniverse, seed: int):
        #: Allows us to print human-readable pair information
        self.pair_universe = pair_universe

        #: Initialize (somewhat) determininistic random number generator
        self.dice = Random(seed)

        #: We operate on daily candles.
        #: At each tick, we process to the next candle
        self.day = 0

        #: Cumulative volume indicator for each of the data feed
        self.indicators = {}
        pair: CapitalgramFeed
        for pair in self.datas:
            self.indicators[pair] = PastTradeVolumeIndicator(pair.lines.volume)

        # How much USD volume token needs to have in order to be eligible for a pick
        self.cumulative_volume_threshold = 500_000

        # How many times we try to pick a token pair to buy
        # before giving up (at early days there might not be enough volume)
        self.pick_attempts = 100

        # If our balance goes below this considering giving up
        self.cash_balance_death_threshold = 100

    def next(self):
        """Tick the strategy.

        Because we are using daily candles, tick will run once per each day.
        """

        # Advance to the next day
        self.day += 1

        # Pick a new token to buy
        for i in range(self.pick_attempts):
            random_pair: CapitalgramFeed = self.dice.choice(self.datas)
            pair_info = random_pair.pair_info
            cum_volume_indicator = self.indicators[random_pair]
            volume = cum_volume_indicator.lines.cum_volume[0]
            if volume > self.cumulative_volume_threshold:
                break
        else:
            print(f"On day #{self.day} did not find any token to buy")
            return

        # Sell any existing token we have
        for ticker in self.datas:
             if self.getposition(ticker).size > 0:
                print(f"On day #{self.day}, selling existing position of {ticker.pair_info}")
                self.close(ticker)

        # Buy in with all money we have.
        # We are not really worried about order size quantisation in crypto.
        cash = self.broker.get_cash()

        if cash < self.cash_balance_death_threshold:
            # We are busted
            return

        # Buy using the daily candle closing price as the rate
        price = random_pair.close[0]
        assert price > 0
        size = cash / price

        # Sell the existing position
        print(f"On day #{self.day} we are buying {pair_info.base_token_symbol} - {pair_info.quote_token_symbol} that has all-time vol of {volume}. Buy in at the close price of {price} {pair_info.base_token_symbol}, cash at hand {cash} USD")

        self.buy(random_pair, size=size, exectype=bt.Order.Market)


def test_backtrader_entropy_monkey(logger, persistent_test_client: Capitalgram):
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
    all_candles = client.fetch_all_candles(CandleBucket.d1).to_pandas()
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
    cerebro.addstrategy(EntropyMonkey, pair_universe=pair_universe, seed=0x1000)

    # Pass all Sushi pairs to the data fees to the strategy
    # noinspection JupyterKernel
    feeds = [df for pair_id, df in sushi_tickers.get_all_pairs()]
    add_dataframes_as_feeds(
        cerebro,
        pair_universe,
        feeds,
        start,
        end,
        CandleBucket.d1)

    # Anaylyse won vs. loss of trades
    cerebro.addanalyzer(analyzers.TradeAnalyzer, _name="tradeanalyzer")  # trade analyzer

    results = cerebro.run()

    strategy: EntropyMonkey = results[0]

    # We run the strategy over 202 days
    assert strategy.day == 244
    trade_analyzer: analyzers.TradeAnalyzer = strategy.analyzers.tradeanalyzer
    assert trade_analyzer.rets["won"]["total"] == 76
    assert trade_analyzer.rets["lost"]["total"] == 89





