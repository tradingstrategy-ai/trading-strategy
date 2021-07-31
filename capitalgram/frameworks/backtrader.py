import datetime
from typing import Iterable

import backtrader as bt
from backtrader.feeds import PandasData
import pandas as pd

from capitalgram.candle import TimeBucket
from capitalgram.pair import DEXPair, PandasPairUniverse


class CapitalgramFeed(PandasData):
    """A Pandas data feed with token metadata added.

    This feed serves Backtrader Cerebro engine. It contains only raw OHLVC info.
    """

    def __init__(self, pair_info: DEXPair):
        self.pair_info = pair_info
        super(CapitalgramFeed, self).__init__()



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

        backtrader_feed = CapitalgramFeed(pair_data, dataname=df, plot=False)
        cerebro.adddata(backtrader_feed)




