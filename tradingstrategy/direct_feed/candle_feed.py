from dataclasses import dataclass
from typing import List, Iterable

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

        :param pairs:
            List of pairs this address contains.

            Symbolic names or addresses.

        :param freq:
            Pandas frequency string e.g. "1H", "min"

        :param candle_offset:
        """
        for p in pairs:
            assert type(p) == str, f"Pairs must be a list of pair ids (str). Got: {p}"
        self.pairs = pairs
        self.timeframe = timeframe
        self.candle_df = pd.DataFrame()
        self.last_cycle = 0

    def apply_delta(self, delta: TradeDelta, label_candles=True):
        """Add new candle data generated from the latest blockchain input.

        :param delta:
            New trades coming in

        :param label_candles:
            Create and update label column.

            Label column contains tooltips for the visual candle viewer.
            This must be done before candle data is grouped by pairs.
        """
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

    def iterate_pairs(self) -> Iterable[pd.DataFrame]:
        """Get candles for all pairs we are tracking."""
        for p in self.pairs:
            yield self.get_candles_by_pair(p)






