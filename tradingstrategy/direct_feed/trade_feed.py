from abc import abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Set, Dict, Optional, Tuple, List, Callable, Iterable, Type, TypeAlias

import pandas as pd
from attr import asdict
from eth_defi.price_oracle.oracle import PriceOracle, BaseOracle

from tqdm import tqdm

from .reorgmon import ReorganisationMonitor


#: Assume we identify our trading pairs by string
PairId: TypeAlias = str


@dataclass(slots=True)
class Trade:
    """Capture information about single trade.

    Designed for technical analysis and trading,
    prices are not intentionally unit accurate and thus
    not suitable for accounting.
    """
    pair: PairId
    block_number: int
    block_hash: str
    timestamp: pd.Timestamp
    tx_hash: str
    log_index: int

    #: Trade price in quote token
    price: Decimal

    #: Trade amount in quote token
    amount: Decimal

    #: The US dollar conversion rate used for this rate
    exchange_rate: Decimal

    @staticmethod
    def get_dataframe_columns() -> dict:
        fields = dict([
            ("pair", "string"),
            ("block_number", "uint64"),
            ("block_hash", "string"),
            ("timestamp", "datetime64[s]"),
            ("tx_hash", "string"),
            ("log_index", "uint32"),
            ("price", "object"),
            ("amount", "object"),
        ])
        return fields



@dataclass(slots=True)
class TradeDelta:
    """Trade history delta for a snapshot"""

    #: Running counter for each connected client
    cycle: int

    #: New incoming data starting this block.
    #:
    #: The client might have already data older than > start_block
    #: from previous delta. In this case the client needs to
    #: purhe  this data as it is discarded due to chain reorganisation.
    start_block: int

    #: Last block for which we have data
    #:
    #:
    end_block: int

    #: List of individual trades
    #:
    trades: pd.DataFrame



class TradeFeed:
    """Real-time blockchain trade feed.

    In-memory latency optimised OHLCv producer for on-chain trades.

    - Keep events in RAM

    - Generate candles based on events

    - Gracefully handle chain reorganisations and drop
      stale data
    """

    def __init__(self,
                 pairs: List[PairId],
                 oracles: Dict[PairId, BaseOracle],
                 reorg_mon: ReorganisationMonitor,
                 data_retention_time: Optional[pd.Timedelta] = None,
                 ):
        """
        Create new real-time OHLCV tracker.

        :param pairs:
            List of pool addresses, or list of similar identifiers.

        :param oracles:
            Reference prices for converting ETH or other crypto quoted
            prices to US dollars.

            In the form of pair -> Price oracle maps.

        :param data_retention_time:
            Discard entries older than this to avoid
            filling the RAM.

        """

        assert isinstance(reorg_mon, ReorganisationMonitor)

        for o in oracles.values():
            assert isinstance(o, BaseOracle)

        self.pairs = pairs
        self.oracles = oracles
        self.data_retention_time = data_retention_time
        self.reorg_mon = reorg_mon
        self.tqdm = tqdm
        self.cycle = 1

        # All event data is stored as dataframe.
        # 1. index is block_number
        # 2. index is log index within the block
        cols = Trade.get_dataframe_columns()
        self.trades_df = pd.DataFrame(columns=list(cols.keys()))

        # Check that every pair has a exchange rate conversion oracle
        for p in self.pairs:
            assert p in self.oracles, f"Pair {p} lacks price oracle. Add oracles by pair identifiers."

    def get_last_block(self) -> Optional[int]:
        """Get the last block number for which we have good data."""

        if len(self.trades_df) == 0:
            return None

        return self.trades_df.iloc[-1]["block_number"]

    def add_trades(self, trades: Iterable[Trade]):
        """Add trade to the ring buffer with support for fixing chain reorganisations.

        Transactions may hop between different blocks when the chain tip reorganises,
        getting a new timestamp. In this case, we update the

        .. note::

            It is safe to call this function multiple times for the same event.

        :return:
            True if the transaction hopped to a different block

        :raise ChainReorganisationDetected:
            If we have detected a block reorganisation
            during importing the data

        """
        data = []

        for evt in trades:

            assert isinstance(evt, Trade)
            assert evt.tx_hash

            self.reorg_mon.check_block_reorg(evt.block_number, evt.block_hash)

            data.append(asdict(evt))

        self.trades_df.append(data)

    def truncate_reorganised_data(self, latest_good_block):
        self.trades_df.truncate(after=latest_good_block)

    def check_reorganisations_and_purge(self) -> int:
        """Check if any of block data has changed

        :return:
            Start block since we need to read new data (inclusive).
        """
        reorg_resolution = self.reorg_mon.update_chain()

        if reorg_resolution.latest_good_block:
            self.truncate_reorganised_data(reorg_resolution.latest_good_block)

        return reorg_resolution.last_block_number

    def backfill_buffer(self, block_count: int, tqdm: Optional[Type[tqdm]] = None) -> TradeDelta:
        """Populate the backbuffer before starting real-time tracker.

        :param block_count:
            Number of blocks we need to fecth

        :param tqdm:
            A progress visualiser.

            Especially useful during the initial fetch, to show the user how
            long it takes time to fill the buffer.

            Must be `tqdm` context manager compatible.

        """
        start_block, end_block = self.reorg_mon.load_initial_data(block_count)
        trades = self.fetch_trades(start_block, end_block, tqdm)
        return self.update_cycle(start_block, end_block, trades)

    def convert_to_dollars(self, pair_address: str, price: Decimal) -> float:
        """Get the trade price as dollars.

        :raise ChainReorganisationDetected:
            If blockchain detects minor reorganisation during the data ignestion
        """
        oracle = self.oracles[pair_address]
        return float(oracle.calculate_price() * price)

    def get_exchange_rate(self, pair: str) -> Decimal:
        """Get the current exchange rate for the pair.

        - There is no block number parameter; We always assume
          the data is filled in order, block by block,
          so the exchange rate is always the exchange rate of the current block.

        - Price oracles are updates per-block before
          trade feed for pairs, so price orocles
          can calculate TWAP or similar exchange rate
          for the block.
        """
        return self.oracles[pair].calculate_price()

    def update_cycle(self, start_block, end_block, trades: Iterable[Trade]) -> TradeDelta:
        """Update the internal work buffer.

        :return:
            Delta of new trades
        """

        self.add_trades(trades)

        res = TradeDelta(
            self.cycle,
            start_block,
            end_block,
            self.trades_df.loc[start_block:end_block+1]
        )

        self.cycle += 1

        return res

    def perform_duty_cycle(self) -> TradeDelta:
        """Update the candle data

        1. Check for block reorganisations

        2. Read new data

        3. Process and index data to candles
        """
        start_block = self.check_reorganisations_and_purge()
        end_block = self.reorg_mon.get_last_block_read()
        trades = self.fetch_trades(start_block, end_block)
        return self.update_cycle(start_block, end_block, trades)

    @abstractmethod
    def fetch_trades(self,
                     start_block: int,
                     end_block: Optional[int],
                     tqdm: Optional[Type[tqdm]] = None
                     ) -> Iterable[Trade]:
        """Read data from the chain.

        Add any new trades using :py:meth:`add_trades`.

        :param start_block:
            Start reading from this block (inclusive)

        :param end_block:
            End at this block (inclusive)

        :param tqdm:
            Optional progress bar displayer gadget.

        :raise ChainReorganisationDetected:
            If blockchain detects minor reorganisation during the data ignestion

        :return:
            TradeDelta instance that contains all new trades since start_block.
        """
