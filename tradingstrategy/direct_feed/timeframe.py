"""Timeframe helpers."""

from dataclasses import dataclass

import pandas as pd


#: Default chart settings for different timeframes.
#:
#: A sensible default of a history for different candle timeframes.
#:
#: Timeframe -> the default history lookback duration mappings.
#: Have around 100 candles per timeframe with the default zoom level.
#:
DEFAULT_DISPLAY_WINDOW_FOR_TIMEFRAME = {
    "1min": pd.Timedelta("2h"),
    "5min": pd.Timedelta("6h"),
    "1h": pd.Timedelta("3d"),
}


@dataclass
class Timeframe:
    """Describe candle timeframe.

    This structure allows us to pass candle resample
    data around the framework.

    This class is very similar to :py:class:`tradingstrategy.timebucket.TimeBucket`,
    but unlike the bucket it allows any time frames presented, not just predefined choices.

    These is also :py:attr:`offset` which allows to shift the starting point of candles
    around.
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

    def __eq__(self, other):
        """Implemented for set() and dict()"""
        assert isinstance(other, Timeframe)
        return self.freq == other.freq and self.offset == other.offset

    def __hash__(self) -> int:
        """Implemented for set() and dict()"""
        return (self.freq, self.offset).__hash__()

    def __repr__(self) -> str:
        return f"<Timeframe freq:{self.freq} offset:{self.offset}>"

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

    def get_default_chart_display_window(self) -> pd.Timedelta:
        """Get the default candle chart time buffer for this frequency.

        See :py:attr:`DEFAULT_DISPLAY_WINDOW_FOR_TIMEFRAME`.

        :return:
            Pandas frequency string
        """
        window = DEFAULT_DISPLAY_WINDOW_FOR_TIMEFRAME.get(self.freq)
        if window:
            return window
        return pd.Timedelta("24h")
