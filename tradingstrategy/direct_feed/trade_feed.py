"""Trade feed.

Blockchain / exchange agnostic trade feed using Pandas :py:class:`DataFrame` as internal memory buffer.e
"""
import logging
from abc import abstractmethod
from dataclasses import dataclass, asdict
from decimal import Decimal
from typing import Dict, Optional, List, Iterable, Type, TypeAlias, Protocol

import pandas as pd
from numpy import isnan
from tqdm import tqdm

from eth_defi.price_oracle.oracle import BasePriceOracle
from eth_defi.event_reader.reorganisation_monitor import ReorganisationMonitor, ChainReorganisationResolution

from .direct_feed_pair import PairId
from .timeframe import Timeframe


logger = logging.getLogger(__name__)


#: Hex presentation of transaction hash, such
Binary256: TypeAlias = str


@dataclass(slots=True, frozen=True)
class Trade:
    """Capture information about single trade.

    Designed for technical analysis and trading,
    prices are not intentionally unit accurate and thus
    not suitable for accounting.
    """

    #: Trading pair id.
    #:
    #: Ethereum address. Always lowercased.
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

    #: The US dollar conversion rate.
    #:
    #: You can use this to convert price and amount to US dollars.
    exchange_rate: Decimal

    def __repr__(self):
        return f"<Trade pair: {self.pair}, price: {self.price}, amount: {self.amount}, exchange rate: {self.exchange_rate}>"

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

    @staticmethod
    def filter_buys(df: pd.DataFrame) -> pd.DataFrame:
        """Filter buy in trades."""
        return df.loc[df.amount > 0]

    @staticmethod
    def filter_sells(df: pd.DataFrame) -> pd.DataFrame:
        """Filter buy in trades."""
        return df.loc[df.amount < 0]


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
    #: This dataframe is snapped to the given time frame to make candle processing easier,
    #: and may contain
    #: historical data from the previous cycles.
    trades: pd.DataFrame

    #: List of trades added in this batch
    #:
    new_trades: pd.DataFrame


class SaveHook(Protocol):
    """Save hook called by trade feed when downloading data.

    Called every N seconds.
    """

    def __call__(self, feed: "TradeFeed"):
        pass


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
                 oracles: Dict[PairId, BasePriceOracle],
                 reorg_mon: ReorganisationMonitor,
                 timeframe: Timeframe = Timeframe("1min"),
                 data_retention_time: Optional[pd.Timedelta] = None,
                 save_hook: Optional[SaveHook] = None,
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

        :param save_hook:
            Sync the downloaded data to disk.

        """

        assert isinstance(reorg_mon, ReorganisationMonitor)

        for o in oracles.values():
            assert isinstance(o, BasePriceOracle)

        for p in pairs:
            assert type(p) == PairId, f"Only ids allowed, got {p}"

        self.pairs = pairs
        self.oracles = oracles
        self.data_retention_time = data_retention_time
        self.reorg_mon = reorg_mon
        self.timeframe = timeframe
        self.cycle = 1
        self.last_save = 0

        self.trades_df = pd.DataFrame()

        # Check that every pair has a exchange rate conversion oracle
        for p in self.pairs:
            assert p in self.oracles, f"Pair {p} lacks price oracle. Add oracles by pair identifiers."

    def __repr__(self):
        if len(self.trades_df) > 0:
            first_ts = self.trades_df.iloc[0]["timestamp"]
            last_ts = self.trades_df.iloc[-1]["timestamp"]
        else:
            first_ts = last_ts = "-"

        return f"<TradeFeed {first_ts} - {last_ts} with {len(self.trades_df)} trades>"

    def get_block_number_of_last_trade(self) -> Optional[int]:
        """Get the last block number for which we have good data."""

        if len(self.trades_df) == 0:
            return None

        return self.trades_df.iloc[-1]["block_number"]

    def get_trade_count(self) -> int:
        """How many trades we track currently."""
        return len(self.trades_df)

    def add_trades(self, trades: Iterable[Trade], start_block: Optional[int]=None, end_block: Optional[int]=None) -> pd.DataFrame:
        """Add trade to the ring buffer with support for fixing chain reorganisations.

        Transactions may hop between different blocks when the chain tip reorganises,
        getting a new timestamp. In this case, we update the

        .. note::

            It is safe to call this function multiple times for the same event.

        :param start_block:

            Expected block range. Inclusive.

            Used for debug assets

        :param end_block:

            Expected block range. Inclusive.

            Used for debug assets

        :return:
            DataFrame of new trades

        :raise ChainReorganisationDetected:
            If we have detected a block reorganisation
            during importing the data

        """
        data = []

        # For each trade added, check that
        for evt in trades:
            assert isinstance(evt, Trade)
            assert type(evt.block_number) == int, f"Got bad block number {evt.block_number} {type(evt.block_number)}"
            self.reorg_mon.check_block_reorg(evt.block_number, evt.block_hash)
            data.append(asdict(evt))

        new_data = pd.DataFrame(data, columns=list(Trade.get_dataframe_columns().keys()))
        new_data.set_index("block_number", inplace=True, drop=False)

        if len(new_data) > 0:

            if start_block:
                min_block = new_data["block_number"].min()
                if isnan(min_block):
                    logger.error("Bad trade event data:")
                    for idx, r in new_data.iterrows():
                        logger.error("%s: %s", idx, r)
                assert min_block >= start_block, f"Trade event outside desired block range. {min_block} earlier than {start_block}"

            if end_block:
                max_block = new_data["block_number"].max()
                assert max_block <= end_block, f"Trade event outside desired block range. {max_block} later than {end_block}"

        # Check that there is no overlap, any block data should not be duplicated
        if len(self.trades_df) > 0 and len(new_data) > 0:
            last_block_in_buffer = self.trades_df.iloc[-1].block_number
            incoming_block = new_data.iloc[0].block_number
            assert incoming_block > last_block_in_buffer, f"Tried to insert existing data. Last block we have {last_block_in_buffer:,}, incoming data starts with block {incoming_block:,}"

        logger.debug("add_trades(): Existing trades: %s, new trades: %s", len(self.trades_df), len(new_data))

        self.check_duplicates_data_frame(new_data)

        self.trades_df = pd.concat([self.trades_df, new_data])

        return new_data

    def get_latest_trades(self, n: int, pair: Optional[PairId] = None) -> pd.DataFrame:
        """Returns the latest trades.

        These trades will be across all trading pairs we are monitoring.

        :param n:
            Number of trades to return

        :param pair:
            Optional pair to filter with

        :return:
            DataFrame containing n trades.

            See :py:class:`Trade` for column descriptions.

            Return empty DataFrame if no trades.
        """

        if len(self.trades_df) == 0:
            return pd.DataFrame()

        if pair:
            df = self.trades_df.loc[self.trades_df["pair"] == pair]
        else:
            df = self.trades_df
        return df.tail(n)

    def get_latest_price(self, pair: PairId) -> Decimal:
        """Return the latest price of a pair.

        Return the last price record we have.
        """
        df = self.trades_df.loc[self.trades_df["pair"] == pair]
        last = df.iloc[-1]
        return last["price"]

    def truncate_reorganised_data(self, latest_good_block):
        """Discard data because of the chain reorg.

        :param latest_good_block:
            The last block that we cannot discard.
        """

        if len(self.trades_df) > 0:
            self.trades_df = self.trades_df.truncate(after=latest_good_block, copy=False)

    def check_current_trades_for_duplicates(self):
        """Check for duplicate trades.

        Internal sanity check - should not happen.

        Dump debug output to error logger if happens.

        :raise AssertionError:
            Should not happen
        """
        self.check_duplicates_data_frame(self.trades_df)

    def check_enough_history(self,
            required_duration: pd.Timedelta,
            now_: Optional[pd.Timestamp] = None,
            tolerance=1.0,
        ):
        """Check that the dafa we have is good time window wise.

        Internal sanity check - should not happen.

        Dump debug output to error logger if happens.

        :param required_duration:
            How far back we need to have data in our buffer.

        :param now_:
            UTC timestamp what's the current time

        :param tolerance:
            How much tolerance we need.

            Default to 100%, no forgiveness for any lack of data.

            There are several reasons for data mismatch, notably
            being that we estimate blockchain timepoints using block ranges with
            expected block time which is not always stable.

        :raise AssertionError:
            If we do not have old enough data
        """

        if not now_:
            now_ = pd.Timestamp.utcnow().tz_localize(None)

        adjusted_duration = (required_duration * tolerance)
        start_threshold = now_ - adjusted_duration
        first_trade = self.trades_df.iloc[0]
        assert first_trade["timestamp"] <= start_threshold, f"We need data to start at {start_threshold}\n" \
                                                            f"Required duration: {required_duration}, adjusted duration: {adjusted_duration}, now: {now_}, tolerance: {tolerance}, but first trade is:\n{first_trade}"

    @staticmethod
    def check_duplicates_data_frame(df: pd.DataFrame):
        """Check data for duplicate trades.

        - Bugs in the event reader system may cause duplicate trades

        - All trades are uniquely identified by tx_hash and log_index

        - In a perfectly working system duplicate trades do not happen

        :param df:
            Input trades

        :raise AssertionError:
            Should not happen
        """

        # https://stackoverflow.com/questions/59618293/how-to-find-duplicates-in-pandas-dataframe-and-print-them
        duplicate_index = df.duplicated(subset=["tx_hash", "log_index"], keep=False)
        duplicates = df[duplicate_index]

        # Output some hints
        if duplicate_index.any():
            for idx, dup in duplicates.iloc[0:3].iterrows():
                logger.error(f"block: {dup.block_number} {dup.block_hash} {dup.log_index} {dup.amount}")

            unique_blocks = df["block_number"].unique()

            logger.error("Total %d duplicates over %d blocks", len(duplicates), len(unique_blocks))
            raise AssertionError(f"Duplicate trades detected in dataframe")

    @staticmethod
    def check_correct_block_range(df: pd.DataFrame, start_block: int, end_block: int):
        """Check that trades in the given DataFrame are for correct block range.

        - Bugs in the event reader system may cause duplicate trades

        - All trades are uniquely identified by tx_hash and log_index

        - In a perfectly working system duplicate trades do not happen

        :param df:
            Input trades

        :param start_block:
            First block we expect tradse to have. Inclusive.

        :param end_block:
            Last block we expect tradse to have. Inclusive.

        :raise AssertionError:
            Should not happen
        """

        if len(df) == 0:
            return

        first_trade_block = df["block_number"].min()
        last_trade_block = df["block_number"].max()

        assert first_trade_block >= start_block, f"First trade at {first_trade_block:,} was outside the range starting at {start_block:,}"
        assert last_trade_block <= end_block, f"Last trade at {last_trade_block:,} was outside the range ending at {end_block:,}"

    def check_reorganisations_and_purge(self) -> ChainReorganisationResolution:
        """Check if any of block data has changed

        :return:
            Start block since we need to read new data (inclusive).
        """
        reorg_resolution = self.reorg_mon.update_chain()
        self.truncate_reorganised_data(reorg_resolution.latest_block_with_good_data)
        return reorg_resolution

    def backfill_buffer(self, block_count: int, tqdm: Optional[Type[tqdm]] = None, save_hook=None) -> TradeDelta:
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

        start_block, end_block = self.reorg_mon.load_initial_block_headers(
            block_count=block_count,
            tqdm=tqdm,
            save_callable=save_hook)

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

    def update_cycle(self,
                     start_block,
                     end_block,
                     reorg_detected,
                     trades: Iterable[Trade]) -> TradeDelta:
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
            Delta of new trades.

            Note that there might not be data for blocks towards `end_block`
            if there were no trades.
        """

        # Update the DataFrame buffer with new trades
        new_trades = self.add_trades(trades, start_block=start_block, end_block=end_block)

        # We need to snap any trade delta update to the edge of candle timeframe
        event_start_ts = self.reorg_mon.get_block_timestamp_as_pandas(start_block)
        data_start_ts = self.timeframe.round_timestamp_down(event_start_ts)
        data_start_block = self.find_first_included_block_in_candle(data_start_ts)

        # If we do not have enough data, then just naively use the given start block
        # and have half-finished candle
        snap_block = data_start_block or start_block

        if len(self.trades_df) > 0:

            # The last block might not contain trades and thus, does not appear in the index.
            # This will cause KeyError in .loc slicing
            last_to_export = min(end_block, self.trades_df.index.values[-1])

            try:
                # Note .loc is inclusive
                # https://medium.com/@curtisringelpeter/understanding-dataframe-selections-and-slices-with-pandas-102a0c2537fb
                # https://medium.com/@curtisringelpeter/understanding-dataframe-selections-and-slices-with-pandas-102a0c2537fb
                exported_trades = self.trades_df.loc[snap_block:last_to_export]
            except KeyError as e:
                # TODO: Figure out
                real_start = self.trades_df.iloc[0]["block_number"]
                real_end = self.trades_df.iloc[-1]["block_number"]
                raise RuntimeError(f"Tried to export trades {snap_block:,} - {last_to_export:,}, but has data {real_start:,} - {real_end:,}") from e
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
            new_trades,
        )

        self.cycle += 1

        return res

    def perform_duty_cycle(self, verbose=False) -> TradeDelta:
        """Update the candle data

        1. Check for block reorganisations

        2. Read new data

        3. Process and index data to candles

        :param verbose:
            More debug logging
        """
        reorg_resolution = self.check_reorganisations_and_purge()
        start_block = reorg_resolution.latest_block_with_good_data + 1
        end_block = self.reorg_mon.get_last_block_read()

        if verbose:
            logger.info(f"Resolved block range to {start_block:,} - {end_block:,}: resolution is {reorg_resolution}")

        # Make sure we do not read ahead of chain tip by accident, because we have forced + 1
        # block above
        if start_block > reorg_resolution.last_live_block:
            start_block = reorg_resolution.last_live_block

        if start_block > end_block:
            # This situation can happen when the lsat block in the chain has reorganised,
            # in such case just read the last block again
            start_block = end_block

        trades = self.fetch_trades(start_block, end_block)

        delta = self.update_cycle(start_block, end_block, reorg_resolution.reorg_detected, trades)
        return delta

    def to_pandas(self, partition_size: int) -> pd.DataFrame:
        df = self.trades_df
        if len(df) > 0:
            df["partition"] = df["block_number"].apply(lambda x: max((x // partition_size) * partition_size, 1))
        return df

    def restore(self, df: pd.DataFrame):
        """Restore data from the previous disk save."""
        logger.debug("Restoring %d trades", len(df))
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
