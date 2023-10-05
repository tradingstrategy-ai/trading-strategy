"""Helper class to manage trading universes."""
from dataclasses import dataclass
from typing import Tuple, Set, Optional

import pandas as pd

from tradingstrategy.candle import GroupedCandleUniverse
from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import Exchange, ExchangeUniverse, ExchangeNotFoundError
from tradingstrategy.lending import LendingReserveUniverse, LendingCandleUniverse
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

    - Trading pairs

    - Lending reserves

    - OHLCV data

    - Liquidity data

    - Lending data

    - Data timeframes (see :py:attr:`time_bucket`)
    """

    #: OHLCV data granularity
    time_bucket: TimeBucket

    #: List of blockchains the strategy trades on
    chains: Set[ChainId]

    #: All the exchanges for this strategy
    #:
    #: Presented with a capsulated :py:class:`ExchangeUniverse` that
    #: offers some convience methods.
    #:
    exchange_universe: Optional[ExchangeUniverse] = None

    #: List of trading pairs the strategy trades on
    pairs: Optional[PandasPairUniverse] = None

    #: Historical candles for the decision making
    candles: Optional[GroupedCandleUniverse] = None

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

    #: Lending rates
    #:
    lending_candles: Optional[LendingCandleUniverse] = None

    #: List of exchanges the strategy trades on.
    #:
    #: TODO: Do not use this - will be be deprecated in the favour of :py:attr:`exchange_universe`
    exchanges: Optional[Set[Exchange]] = None

    def __post_init__(self):
        """Check that the constructor was called correctly."""
        if self.candles is not None:
            assert isinstance(self.candles, GroupedCandleUniverse), f"Expected GroupedCandleUniverse, got {self.candles.__class__}"

        if self.pairs is not None:
            assert isinstance(self.pairs, PandasPairUniverse), f"Expected PandasPairUniverse, got {self.pairs.__class__}"

        if self.exchanges is not None:
            # TODO: Legacy
            assert isinstance(self.exchanges, dict), f"Expected dict, got {self.exchanges.__class__}"

        if self.exchange_universe is not None:
            # TODO: Legacy
            assert isinstance(self.exchange_universe, ExchangeUniverse), f"Expected dict, got {self.exchanges.__class__}"

        if self.lending_candles is not None:
            assert isinstance(self.lending_candles, LendingCandleUniverse), f"Expected LendingCandleUniverse, got {self.exchanges.__class__}"

    @property
    def lending_reserves(self) -> LendingReserveUniverse:
        """Each lending metric is paired with a copy of the universe"""
        return self.lending_candles.lending_reserves

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

        :raise: ExchangeNotFoundError if exchange not found

        :return: Exchange instance
        """
        for exc in self.exchanges:
            if exc.exchange_id == id:
                return exc
        
        raise ExchangeNotFoundError(exchange_id=id) 

