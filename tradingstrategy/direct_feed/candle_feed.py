from dataclasses import dataclass

import pandas as pd

from tradingstrategy.direct_feed.trade_feed import TradeDelta, PairId


@dataclass(slots=True, frozen=True)
class OHLCVCandle:
    """One OHLCV candle in the direct data feed."""

    pair: PairId
    timestamp: pd.Timestamp
    start_block: int
    end_block: int

    open: float
    high: float
    low: float
    close: float
    volume: float
    exchange_rate: float

    @staticmethod
    def get_dataframe_columns() -> dict:
        fields = dict([
            ("pair", "string"),
            ("start_block", "uint64"),
            ("end_block", "uint64"),
            ("open", "float32"),
            ("high", "float32"),
            ("low", "float32"),
            ("close", "float32"),
            ("volume", "float32"),
            ("exchange_rate", "float32"),
        ])
        return fields


class CandleFeed:
    """Create candles for certain time frame for multiple pairs.

    - Takes :py:class:`TradeFeed` as input

    - Generates candles based on this feed

    - Can only generate candles of one duration
    """

    def __init__(self,
                 freq: str,
                 candle_offset: pd.Timedelta,
                 ):
        """

        :param freq:
            Pandas frequency string e.g. "1H", "min"

        :param candle_offset:
        """
        self.freq = freq
        self.candle_offset = candle_offset
        self.df = pd.DataFrame()

    def truncate(self, df: pd.DataFrame, start_block: int) -> pd.DataFrame:
        """Truncate the existing store """

    def append_delta(self, delta: TradeDelta):
        """Generate candles."""
        latest_good_block = delta.start_block - 1
        df = self.trades_df.truncate(after=latest_good_block, copy=False)

    def get_candles(self, pair: PairId) -> pd.DataFrame:
        pass



