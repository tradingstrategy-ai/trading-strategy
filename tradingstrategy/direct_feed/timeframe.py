"""Timeframe helpers."""

from dataclasses import dataclass

import pandas as pd


@dataclass
class Timeframe:
    """Describe candle timeframe.

    This structure allows us to pass candle resample
    data around the framework.
    """

    #: Pandas frequency string.
    #:
    #: E.g. `1D` for daily, `1min` for minute.
    #:
    #: See https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
    freq: str

    #: Candle shift offset
    #:
    #: E.g. move hourly candles 5 minutes backwards to start at 00:55
    offset: pd.Timedelta = pd.Timedelta(seconds=0)

    def round_timestamp_down(self, ts: pd.Timestamp) -> pd.Timestamp:
        """Snap to previous available timedelta.

        Preserve any timezone info on `ts`.

        If we are at the the given exact delta, then do not round, only add offset.

        :param ts:
            Timestamp we want to round

        :return:
            When to wake up from the sleep next time
        """
        return ts.floor(self.freq) + self.offset
