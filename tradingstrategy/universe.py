"""Helper class to manage trading universes."""
from dataclasses import dataclass
from typing import Tuple, Set, Optional

import pandas as pd

from tradingstrategy.candle import GroupedCandleUniverse
from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import Exchange, ExchangeUniverse
from tradingstrategy.liquidity import GroupedLiquidityUniverse, ResampledLiquidityUniverse
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket


@dataclass
class Universe:
    """Trading universe description.

    Encapsulates all the data we need to make trading decisions.
    A trading strategy or a research notebook can use this
    class to pass around information of its available data.

    This includes

    - blockchains

    - exchanges

    - trading pairs

    - OHLCV data

    - Liquidity data

    - Data timeframes (see :py:attr:`time_bucket`)
    """

    #: OHLCV data granularity
    time_bucket: TimeBucket

    #: List of blockchains the strategy trades on
    chains: Set[ChainId]

    #: List of exchanges the strategy trades on.
    #:
    #: TODO: Currently not suitable for large number of exchanges in the same strategy.
    #:
    #: TODO: Do not use this - will be be deprecated in the favour of :py:attr:`exchange_universe`
    exchanges: Set[Exchange]

    #: List of trading pairs the strategy trades on
    pairs: PandasPairUniverse

    #: Historical candles for the decision making
    candles: GroupedCandleUniverse

    #: Liquidity data granularity
    liquidity_time_bucket: Optional[TimeBucket] = None

    #: Historical liquidity samples.
    #:
    #: Might not be loaded if the strategy does not need to access
    #: liquidity data.
    liquidity: Optional[GroupedLiquidityUniverse] = None

    #: Historical liquidity samples, resampled for backtesting speed.
    #:
    #: As strategies often do not need accurate liquidity information,
    #: approximation is enough, this resampled liquidity is
    #: optimised for backtesting speed.
    #:
    resampled_liquidity: Optional[ResampledLiquidityUniverse] = None

    #: All the exchanges for this strategy
    #:
    #: Presented with a capsulated :py:class:`ExchangeUniverse` that
    #: offers some convience methods.
    #:
    #: TODO: This is a new attribute - not available through all code paths yet.
    exchange_universe: Optional[ExchangeUniverse] = None

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

