from typing import Iterable

import backtrader as bt
import pandas as pd


def prepare_candles_for_backtrader(candles: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame format so that Backtrader strategy can read it.

    What assumptions :py:method:`Celebro.addfeed` makes about Pandas data.
    """

    # Our index must be the timestamp
    candles = candles.set_index(pd.DatetimeIndex(candles["timestamp"]))

    # Create volume column
    candles["volume"] = candles["buy_volume"] + candles["sell_volume"]

    return candles


def add_dataframes_as_feeds(cerebro: bt.Cerebro, datas: Iterable[pd.DataFrame]):
    """Add Pandas candle data as source feed to Backtrader strategy tester."""

    datas = list(datas)

    # TODO HAX
    # Backtrader cannot iterate over broken data if some feeds have data and some do not.
    # With unluck Backtrader stops at the first empty day of a random feed.
    # We mitigate this by assuming the longest feed has all days and
    # longest feed is the first
    datas = sorted(datas, key=lambda df: len(df), reverse=True)

    # TODO: Seems to be really hard to get Backtrader to accept gapepd data
    # Maybe to write in a custom feed class?
    for df in datas:
        backtrader_feed = bt.feeds.PandasData(dataname=df)
        cerebro.adddata(backtrader_feed)
        # cerebro.resampledata(backtrader_feed)
        break



