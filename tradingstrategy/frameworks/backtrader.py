"""Old Backtrader integration.

.. warning::

    Deprecased. Do not use anymore. Use `trade-executor` framewor instead.
"""

import logging
import datetime
from typing import Iterable, List, Optional, Union, Set

import backtrader as bt
from backtrader import Trade, LineIterator, TradeHistory, num2date, BuyOrder, SellOrder, AutoOrderedDict
from backtrader.feeds import PandasData
import pandas as pd

from tradingstrategy.analysis.tradeanalyzer import TradeAnalyzer, AssetTradeHistory, SpotTrade
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.pair import DEXPair, PandasPairUniverse


logger = logging.getLogger(__name__)


def convert_backtrader_timestamp(bt_time: float) -> pd.Timestamp:
    """Convert traderader internal timestamps to Pandas.

    Backtrader internally tracks time as "float of day numbers."
    Go figures?

    See :py:meth:`PandasData._load` for more information.

    See :py:func:`num2date` for more information.
    """
    dt = num2date(bt_time)
    return pd.Timestamp(dt)


class DEXFeed(PandasData):
    """A Pandas data feed with token metadata added.

    This feed serves Backtrader Cerebro engine. It contains only raw OHLVC info.
    """

    def __init__(self, pair_info: DEXPair):
        self.pair_info = pair_info
        super(DEXFeed, self).__init__()


class DEXStrategy(bt.Strategy):
    """A strategy base class with support for Trading Strategy DEX specific use cases."""

    def __init__(self, *args, **kwargs):
        super(DEXStrategy, self).__init__(*args, **kwargs)

        #: Currently open position
        self.last_opened_buy: Optional[BuyOrder] = None

        #: The next() tick counter
        self.tick: Optional[int] = None

        self._tradehistoryon = True

    def buy(self, *args, **kwargs) -> BuyOrder:
        """Stamps each trade with a timestamp.

        Normal Backtrader does not have this functionality.
        """

        assert "hint" in kwargs, "Please give hint parameter to buy()"

        buy: BuyOrder = super().buy(*args, **kwargs)
        # Save the trade for the stop loss management
        self.last_opened_buy = buy

        # Hint will be available as order.info.hint
        return buy

    def close(self, *args, **kwargs):
        ret = super().close(*args, **kwargs)
        self.last_opened_buy = None
        return ret

    def get_timestamp(self) -> pd.Timestamp:
        """Get the timestamp of the current candle"""
        bt_time = self.datetime[0]
        return convert_backtrader_timestamp(bt_time)

    def _start(self):
        """"Add tick counter"""
        super()._start()
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


def reindex_pandas_for_backtrader(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, bucket: TimeBucket):
    """Backtrader does not allow sparse data, but all data must be filled.

    Sparse data: not trades at the beginning, end of the time series. Missed trading days.
    """

    freq = bucket.to_frequency()
    idx = pd.date_range(start, end, freq=freq)

    # https://stackoverflow.com/a/19324591/315168
    # https://stackoverflow.com/questions/47231496/pandas-fill-missing-dates-in-time-series
    # df.index = idx
    # Backtrader only cares about OHLCV values,
    # so we set everything to zero on missing days
    # TODO: Copy previous day open/close/high/etc here.
    # df.index = pd.DatetimeIndex(df.index)
    reindexed = df.reindex(idx, fill_value=0)
    return reindexed


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

        # Drop unnecessary columns
        df = df[["open", "high", "low", "close", "volume"]]

        # Reindex so that backtrader can read data
        df = reindex_pandas_for_backtrader(df, start, end, bucket)

        backtrader_feed = DEXFeed(pair_data, dataname=df, plot=plot)
        cerebro.adddata(backtrader_feed)


def analyse_strategy_trades(trades: List[Trade]) -> TradeAnalyzer:
    """Build a trade analyzer from Backtrader executed portfolio."""
    histories = {}

    trade_id = 1

    # Each Backtrader Trade instance presents a position
    # Trade instances contain TradeHistory entries that present change to this position
    # with Order instances attached
    for t in trades:

        assert t.historyon, "Trade history must be on in Backtrader to analyse trades"

        feed: DEXFeed = t.data
        pair_info = feed.pair_info
        pair_id = pair_info.pair_id
        assert type(pair_id) == int

        histentry: TradeHistory
        for histentry in t.history:

            logger.debug("Processing history entry %s: %s", t.tradeid, histentry)

            history = histories.get(pair_id)
            if not history:
                history = histories[pair_id] = AssetTradeHistory()

            status = histentry.status

            assert status.status in (Trade.Open, Trade.Closed), f"Got status {status}"

            event: AutoOrderedDict = histentry.event
            order: Union[BuyOrder, SellOrder] = event.order

            # Internally negative quantities are for sells
            quantity = event.size
            timestamp = convert_backtrader_timestamp(status.dt)
            price = event.price or status.price or order.price or 0
            # price = order.price
            hint = histentry.event.order.info.hint

            # print("Got event", event, status)
            assert quantity != 0, f"Got bad quantity for {status}"
            # import ipdb ; ipdb.set_trace()
            assert price > 0, f"Got invalid trade event {event}, status {status}, order {order}"

            trade = SpotTrade(
                pair_id=pair_id,
                trade_id=trade_id,
                timestamp=timestamp,
                price=price,
                quantity=quantity,
                commission=event.commission,
                slippage=0,  # TODO
                hint=hint,
            )
            history.add_trade(trade)
            trade_id += 1

    return TradeAnalyzer(asset_histories=histories)


class TradeRecorder(bt.Analyzer):
    """Record all trades during the backtest run so that they can be analysed."""

    def create_analysis(self):
        self.trades: List[Trade] = []
        self.existing_trades: Set[int] = set()

    def stop(self):
        pass

    def notify_trade(self, trade: Trade):
        assert isinstance(trade, Trade)
        if trade.ref not in self.existing_trades:
            # One trade got multiple notifications,
            # see if start tracking a new trade
            self.trades.append(trade)
            self.existing_trades.add(trade.ref)

    def get_analysis(self) -> dict:
         # Internally called by Backtrader
         return {"trades": self.trades}

