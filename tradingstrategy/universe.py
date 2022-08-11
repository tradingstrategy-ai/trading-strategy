"""Helper class to manage trading universes."""
from dataclasses import dataclass
from typing import Tuple, Set, Optional

import pandas as pd

from tradingstrategy.candle import GroupedCandleUniverse
from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import Exchange
from tradingstrategy.liquidity import GroupedLiquidityUniverse
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket


@dataclass
class Universe:
    """Set up trading universe.

    Encapsulates all the data we need to make trading decisions.
    This includes blockchains, exchanges, trading pairs, etc.
    """

    time_bucket: TimeBucket

    #: List of blockchains the strategy trades on
    chains: Set[ChainId]

    #: List of exchanges the strategy trades on.
    #: TODO: Currently not suitable for large number of exchanges in the same strategy.
    exchanges: Set[Exchange]

    #: List of trading pairs the strategy trades on
    pairs: PandasPairUniverse

    #: Historical candles for the decision making
    candles: GroupedCandleUniverse

    #: Historical liquidity samples.
    #: Might not be loaded if the strategy does not need to access
    #: liquidity data.
    liquidity: Optional[GroupedLiquidityUniverse] = None

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

    def get_exchange_by_id(self, id: int) -> Optional[Exchange]:
        """Get exchange by its id.

        TODO: Replace exchange set with a proper exchange universe.
        """
        for exc in self.exchanges:
            if exc.exchange_id == id:
                return exc
        return None

