from abc import abstractmethod
from dataclasses import dataclass, asdict
from decimal import Decimal
from typing import Dict, Optional, List,  Iterable, Type, TypeAlias

import pandas as pd
from eth_defi.price_oracle.oracle import BaseOracle

from tqdm import tqdm

from .direct_feed_pair import PairId
from .reorg_mon import ReorganisationMonitor, ChainReorganisationResolution
from .timeframe import Timeframe


#: Hex presentation of transaction hash, such
Binary256: TypeAlias = str


@dataclass(slots=True, frozen=True)
class Trade:
    """Capture information about single trade.

    Designed for technical analysis and trading,
    prices are not intentionally unit accurate and thus
    not suitable for accounting.
    """

    #: Trading pair od
    pair: PairId

    #: Block number
    block_number: int

    #: Block hash
    block_hash: str

    #: Block mined at
    timestamp: pd.Timestamp

    #: Block mined at
    tx_hash: Binary256

    #: Trade index within the block
    log_index: int

    #: Trade price in quote token
    price: Decimal

    #: Trade amount in quote token.
    #:
    #: Positive for buys, negative for sells.
    amount: Decimal

    #: The US dollar conversion rate used for this rate
    exchange_rate: Decimal

    def __post_init__(self):
        assert self.timestamp.tzinfo is None, "Don't use timezone aware timestamps - everything must be naive UTC"
        assert self.tx_hash, "tx_hash missing"

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
            ("exchange_rate", "object"),
        ])
        return fields


@dataclass(slots=True, frozen=True)
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

    # The start block we are using if we do not do timeframe snap.
    #
    unadjusted_start_block: int

    #: Last block for which we have data
    #:
    #:
    end_block: int

    #: Timestamp of the start block
    #:
    start_ts: pd.Timestamp

    #: Timestamp of the end block
    #:
    end_ts: pd.Timestamp

    #: Did we detect any chain reorgs on this update cycle?
    #:
    reorg_detected: bool

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
                 timeframe: Timeframe = Timeframe("1min"),
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

        :param reorg_mon:
            Reorganisation detector and last good block state for a chain.

        :param timeframe:
            Expected timeframe of the data.
            Any trade deltas are snapped to the exact timeframe,
            so that when candle data gets updated,
            we always update only full candles and
            never return trade data that would break the timeframe in a middle.

            Default to one minute candles.

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
        self.timeframe = timeframe
        self.cycle = 1

        self.trades_df = pd.DataFrame()

        # Check that every pair has a exchange rate conversion oracle
        for p in self.pairs:
            assert p in self.oracles, f"Pair {p} lacks price oracle. Add oracles by pair identifiers."

    def get_block_number_of_last_trade(self) -> Optional[int]:
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
            self.reorg_mon.check_block_reorg(evt.block_number, evt.block_hash)
            data.append(asdict(evt))

        new_data = pd.DataFrame(data, columns=list(Trade.get_dataframe_columns().keys()))
        new_data.set_index("block_number", inplace=True, drop=False)
        self.trades_df = pd.concat([self.trades_df, new_data])

    def get_latest_trades(self, n: int, pair: Optional[PairId] = None) -> pd.DataFrame:
        """Returns the latest trades.

        These trades will be across all trading pairs we are monitoring.

        :param n:
            Number of trades to return

        :param pair:
            Optional pair to filter with

        :return:
            DataFrame containing n trades
        """
        if pair:
            df = self.trades_df.loc[self.trades_df["pair"] == pair]
        else:
            df = self.trades_df
        return df.tail(n)

    def truncate_reorganised_data(self, latest_good_block):
        """Discard data because of the chain reorg.

        :param latest_good_block:
            The last block that we cannot discard.
        """

        if len(self.trades_df) > 0:
            self.trades_df = self.trades_df.truncate(after=latest_good_block, copy=False)

    def check_reorganisations_and_purge(self) -> ChainReorganisationResolution:
        """Check if any of block data has changed

        :return:
            Start block since we need to read new data (inclusive).
        """
        reorg_resolution = self.reorg_mon.update_chain()
        self.truncate_reorganised_data(reorg_resolution.latest_block_with_good_data)
        return reorg_resolution

    def backfill_buffer(self, block_count: int, tqdm: Optional[Type[tqdm]] = None) -> TradeDelta:
        """Populate the backbuffer before starting real-time tracker.

        :param block_count:
            Number of blocks we need to fetch

        :param tqdm:
            A progress visualiser.

            Especially useful during the initial fetch, to show the user how
            long it takes time to fill the buffer.

            Must be `tqdm` context manager compatible.

        :return:
            Data loaded and filled to the work buffer.
        """
        start_block, end_block = self.reorg_mon.load_initial_data(block_count)
        trades = self.fetch_trades(start_block, end_block, tqdm)
        # On initial load, we do not care about reorgs
        return self.update_cycle(start_block, end_block, False, trades)

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

    def find_first_included_block_in_candle(self, ts: pd.Timestamp) -> Optional[int]:
        """Find the first block number that contains data going into the candle.

        :param ts:
            Timestamp when the block can start (inclusive)

        :return:
            Block number.

            If there is no data, return None.
        """

        after_df = self.trades_df.loc[self.trades_df["timestamp"] >= ts]

        if len(after_df) > 0:
            return after_df.iloc[0]["block_number"]
        else:
            return None

    def update_cycle(self, start_block, end_block, reorg_detected, trades: Iterable[Trade]) -> TradeDelta:
        """Update the internal work buffer.

        - Adds the new trades to the work buffer

        - Updates the cycle number

        - Creates the snapshot of the new trades for the client

        :param start_block:
            Incoming new data, first block (inclusive)

        :param end_block:
            Incoming new data, last  block (inclusive)

        :param reorg_detected:
            Did we detect any chain reorganisations in this cycle

        :param trades:
            Iterable o new trades

        :return:
            Delta of new trades
        """

        self.add_trades(trades)

        # We need to snap any trade delta update to the edge of candle timeframe
        event_start_ts = self.reorg_mon.get_block_timestamp_as_pandas(start_block)
        data_start_ts = self.timeframe.round_timestamp_down(event_start_ts)
        data_start_block = self.find_first_included_block_in_candle(data_start_ts)

        # If we do not have enough data, then just naively use the given start block
        # and have half-finished candle
        snap_block = data_start_block or start_block

        if len(self.trades_df) > 0:
            exported_trades = self.trades_df.loc[snap_block:end_block+1]
        else:
            exported_trades = pd.DataFrame()

        start_ts = pd.Timestamp.utcfromtimestamp(self.reorg_mon.get_block_timestamp(snap_block)).tz_localize(None)
        end_ts = pd.Timestamp.utcfromtimestamp(self.reorg_mon.get_block_timestamp(snap_block)).tz_localize(None)

        res = TradeDelta(
            self.cycle,
            snap_block,
            start_block,
            end_block,
            start_ts,
            end_ts,
            reorg_detected,
            exported_trades,
        )

        self.cycle += 1

        return res

    def perform_duty_cycle(self) -> TradeDelta:
        """Update the candle data

        1. Check for block reorganisations

        2. Read new data

        3. Process and index data to candles
        """
        reorg_resolution = self.check_reorganisations_and_purge()
        start_block = reorg_resolution.latest_block_with_good_data + 1
        end_block = self.reorg_mon.get_last_block_read()

        if start_block > end_block:
            # This situation can happen when the lsat block in the chain has reorganised,
            # in such case just read the last block again
            start_block = end_block

        trades = self.fetch_trades(start_block, end_block)
        return self.update_cycle(start_block, end_block, reorg_resolution.reorg_detected, trades)

    def restore(self, df: pd.DataFrame):
        """Restore data from the previous disk save."""
        self.trades_df = df

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
