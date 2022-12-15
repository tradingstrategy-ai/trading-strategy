from dataclasses import dataclass
from typing import List

import pandas as pd

from tradingstrategy.direct_feed.ohlcv_aggregate import resample_trades_into_ohlcv, get_feed_for_pair, truncate_ohlcv
from tradingstrategy.direct_feed.timeframe import Timeframe
from tradingstrategy.direct_feed.trade_feed import TradeDelta
from tradingstrategy.direct_feed.direct_feed_pair import PairId


class CandleFeed:
    """Create candles for certain time frame for multiple pairs.

    - Takes :py:class:`TradeFeed` as input

    - Generates candles based on this feed

    - Can only generate candles of one duration
    """

    def __init__(self,
                 pairs: List[PairId],
                 timeframe: Timeframe,
                 ):
        """

        :param freq:
            Pandas frequency string e.g. "1H", "min"

        :param candle_offset:
        """
        self.timeframe = timeframe
        self.candle_df = pd.DataFrame()
        self.last_cycle = 0

    def apply_delta(self, delta: TradeDelta):
        """Add new candle data generated from the latest blockchain input."""
        cropped_df = truncate_ohlcv(self.candle_df, delta.start_ts)
        candles = resample_trades_into_ohlcv(delta.trades, self.timeframe)
        self.candle_df = pd.concat([cropped_df, candles])
        self.last_cycle = delta.cycle

    def get_candles_by_pair(self, pair: PairId) -> pd.DataFrame:
        return get_feed_for_pair(self.candle_df, pair)

    def get_last_block_number(self) -> int:
        """Get overall last block number for which we have valid data.

        :return:
            block number (inclusive)
        """
        return self.candle_df["end_block"].max()






