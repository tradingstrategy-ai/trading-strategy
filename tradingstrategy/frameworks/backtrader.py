"""Helper methods and classes to integrate :term:`Backtrader` with Capitalgram based :term:`Pandas` data."""

import datetime
from typing import Iterable, List

import backtrader as bt
from backtrader import Trade, LineIterator
from backtrader.feeds import PandasData
import pandas as pd

from tradingstrategy.analysis.tradeanalyzer import TradeAnalyzer, AssetTradeHistory, SpotTrade
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.pair import DEXPair, PandasPairUniverse


class CapitalgramFeed(PandasData):
    """A Pandas data feed with token metadata added.

    This feed serves Backtrader Cerebro engine. It contains only raw OHLVC info.
    """

    def __init__(self, pair_info: DEXPair):
        self.pair_info = pair_info
        super(CapitalgramFeed, self).__init__()


class DEXStragety(bt.Strategy):
    """A strategy base class with support for Trading Strategy DEX specific use cases."""

    def buy(self, *args, **kwargs):
        """Stamps each trade with a timestamp.

        Normal Backtrader does not have this functionality.
        """
        trade: Trade = super(self).buy(*args, **kwargs)
        import ipdb ; ipdb.set_trace()
        self.last_trade = trade

    def close(self, *args, **kwargs):
        super(self).close(*args, **kwargs)
        self.last_trade = None

    def get_timestamp(self) -> pd.Timestamp:
        return pd.Timestamp.utcfromtimestamp(self.datetime[0])

    def _start(self):
        """"Add tick counter"""
        super(self)._start(self)
        self.tick = 0

    def _oncepost(self, dt):
        """Add tick counter."""
        for indicator in self._lineiterators[LineIterator.IndType]:
            if len(indicator._clock) > len(indicator):
                indicator.advance()

        if self._oldsync:
            # Strategy has not been reset, the line is there
            self.advance()
        else:
            # strategy has been reset to beginning. advance step by step
            self.forward()

        self.lines.datetime[0] = dt
        self._notify()

        minperstatus = self._getminperstatus()
        if minperstatus < 0:
            self.tick += 1
            self.next()
        elif minperstatus == 0:
            self.nextstart()  # only called for the 1st value
        else:
            self.prenext()

        self._next_analyzers(minperstatus, once=True)
        self._next_observers(minperstatus, once=True)

        self.clear()


def prepare_candles_for_backtrader(candles: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame format so that Backtrader strategy can read it.

    What assumptions :py:meth:`Celebro.addfeed` makes about Pandas data.
    """

    # Our index must be the timestamp
    candles = candles.set_index(pd.DatetimeIndex(candles["timestamp"]))

    # Create volume column
    candles["volume"] = candles["buy_volume"] + candles["sell_volume"]

    return candles


def reindex_pandas_for_backtrader(df: pd.DataFrame, start, end, bucket):
    """Backtrader does not allow sparsedata, but all data must be filled"""

    # https://stackoverflow.com/a/19324591/315168
    # https://stackoverflow.com/questions/47231496/pandas-fill-missing-dates-in-time-series
    assert bucket == TimeBucket.d1, "Only daily candles supported ATM"
    idx = pd.date_range(start, end)
    # df.index = idx
    # Backtrader only cares about OHLCV values,
    # so we set everything to zero on missing days
    # TODO: Copy previous day open/close/high/etc here.
    df.index = pd.DatetimeIndex(df.index)
    return df.reindex(idx, fill_value=0)


def add_dataframes_as_feeds(
        cerebro: bt.Cerebro,
        pair_universe: PandasPairUniverse,
        datas: Iterable[pd.DataFrame],
        start: datetime.datetime,
        end: datetime.datetime,
        bucket: TimeBucket,
        plot=False):
    """Add Pandas candle data as source feed to Backtrader strategy tester.

    For each py:class:`pd.DataFrame` creates a new :py:meth:`bt.Celebro.adddata` feed
    of the type :py:class:`CapitalgramFeed`.
    Data on any missing dates is gracefully handled.

    :param plot: Whether Backtrader includes this series in its default plot
    """

    datas = list(datas)

    # TODO HAX
    # Backtrader cannot iterate over broken data if some feeds have data and some do not.
    # With unluck Backtrader stops at the first empty day of a random feed.
    # We mitigate this by assuming the longest feed has all days and
    # longest feed is the first
    datas = sorted(datas, key=lambda df: len(df), reverse=True)

    for df in datas:
        pair_id = df["pair_id"][0]
        pair_data = pair_universe.get_pair_by_id(pair_id)

        # Drop unnecessary columsn
        df = df[["open", "high", "low", "close", "volume"]]

        # Reindex so that backtrader can read data
        df = reindex_pandas_for_backtrader(df, start, end, bucket)

        backtrader_feed = CapitalgramFeed(pair_data, dataname=df, plot=plot)
        cerebro.adddata(backtrader_feed)


def analyse_strategy_trades(trades: List[Trade]) -> TradeAnalyzer:
    """Build a trade analyzer from Backtrader executed portfolio."""
    histories = {}

    trade_id = 1

    import ipdb ; ipdb.set_trace()

    for t in trades:

        feed: CapitalgramFeed = t.data
        pair_info = feed.pair_info

        pair_id = pair_info.pair_id
        assert type(pair_id) == int
        history = histories.get(pair_id)
        if not history:
            history = histories[pair_id] = AssetTradeHistory()

        trade = SpotTrade(
            pair_id=pair_id,
            trade_id=trade_id,
            timestamp=txn.dt,
            price=txn.price,
            quantity=txn.quantity,
            commission=0,
            slippage=0,
        )
        assert txn.quantity
        assert txn.price
        history.add_trade(trade)
        trade_id += 1

    return TradeAnalyzer(asset_histories=histories)


class TradeRecorder(bt.Analyzer):
    """Record all trades during the backtest run so that they can be analysed."""

    def create_analysis(self):
        self.trades: List[Trade] = []

    def stop(self):
        pass

    def notify_trade(self, trade: Trade):
        assert isinstance(trade, Trade)
        self.trades.append(trade)

    def get_analysis(self) -> dict:
         # Internally called by Backtrader
         return {"trades": self.trades}

