"""Helper class to manage trading universes."""
from dataclasses import dataclass
from typing import List, Tuple

import pandas as pd

from tradingstrategy.candle import GroupedCandleUniverse
from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import Exchange
from tradingstrategy.liquidity import GroupedLiquidityUniverse
from tradingstrategy.pair import DEXPair, PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket


@dataclass
class Universe:
    """Set up trading universe.

    Encapsulates all the data we need to make trading decisions.
    This includes blockchains, exchanges, trading pairs, etc.
    """

    time_frame: TimeBucket

    #: List of blockchains the strategy trades on
    chains: List[ChainId]

    #: List of exchanges the strategy trades on
    exchanges: List[Exchange]

    #: List of trading pairs the strategy trades on
    pairs: PandasPairUniverse

    #: Historical candles for the decision making
    candles: GroupedCandleUniverse

    #: Historical liquidity sampels
    liquidity: GroupedLiquidityUniverse

    def get_candle_availability(self) -> Tuple[pd.Timestamp, pd.Timestamp]:
        """Get the time range for which we have candle data.

        Useful to check if the data is out of date.

        :return: start,end range
        """
        return self.candles.get_timestamp_range()

    def get_single_exchange(self) -> Exchange:
        """For strategies that use only one exchange, get the exchange instance.

        :raise: AssertationError if multiple exchanges preset
        """
        assert len(self.exchanges) == 1
        return self.exchanges[0]

