import datetime
import enum
from decimal import Decimal
from typing import List, Dict, Tuple, Optional, Iterable, Type, Set
import logging

import pandas as pd
from tqdm import tqdm

from eth_defi.abi import get_contract
from eth_defi.event_reader.conversion import decode_data, convert_int256_bytes_to_int, convert_jsonrpc_value_to_int
from eth_defi.event_reader.filter import Filter
from eth_defi.event_reader.logresult import LogResult, LogContext
from eth_defi.event_reader.reader import read_events_concurrent, read_events
from eth_defi.event_reader.web3factory import Web3Factory
from eth_defi.event_reader.web3worker import create_thread_pool_executor
from eth_defi.price_oracle.oracle import PriceOracle, BasePriceOracle
from eth_defi.uniswap_v2.pair import PairDetails
from eth_defi.event_reader.reorganisation_monitor import ReorganisationMonitor

from .timeframe import Timeframe
from .trade_feed import Trade, TradeFeed


logger = logging.getLogger(__name__)


#: List of output columns to pairs.csv
PAIR_FIELD_NAMES = [
    "block_number",
    "timestamp",
    "tx_hash",
    "log_index",
    "factory_contract_address",
    "pair_contract_address",
    "pair_count_index",
    "token0_address",
    "token0_symbol",
    "token1_address",
    "token1_symbol",
]

#: List of fields we need to decode in swaps
SWAP_FIELD_NAMES = [
    "block_number",
    "timestamp",
    "tx_hash",
    "log_index",
    "pair_contract_address",
    "amount0_in",
    "amount1_in",
    "amount0_out",
    "amount1_out",
]

#: List of fields we need to decode in syncs
SYNC_FIELD_NAMES = [
    "block_number",
    "timestamp",
    "tx_hash",
    "log_index",
    "pair_contract_address",
    "reserve0",
    "reserve1",
]


class SwapKind(enum.Enum):
    """What kind of swaps we might have."""

    # token1 -> token0
    buy = "buy"

    # token0 -> token1
    sell = "sell"

    # Traded both ways at the same time
    complex = "complex"

    # Zero traded volumne
    invalid = "invalid"


class UniswapV2TradeFeed(TradeFeed):
    """Uniswap v2 compatible DXE candle generator.

    Uses multiple threads to speed up blockchain read.
    """

    def __init__(self,
                 pairs: List[PairDetails],
                 web3_factory: Web3Factory,
                 oracles: Dict[str, PriceOracle],
                 reorg_mon: ReorganisationMonitor,
                 timeframe: Timeframe,
                 data_retention_time: Optional[pd.Timedelta] = None,
                 threads=10,
                 chunk_size=100):
        """

        :param pairs:
            List of Uniswap v2 pool addresses

        :param web3_factory:
            Web3 connecion creator (multithread safe)

        :param oracles:
            Price oracles needed for the exchange rate conversion

        :param timeframe:
            Maximum timeframe for produced candles
            when doing candle refreshes.

            This must be the width of the buffer so that we can render
            the candles at their longest resolution. E.g.
            get at least 50 hourly candles when 1h candles are rendered,
            which means we need to have data for ~2 days in the buffer.

        :param reorg_mon:
            Chain reorganistaion manager

        :param data_retention_time:

        :param threads:
            Number of threads used in the reader pool.
            Set 1 to disable thread pooling.

        :param chunk_size:
            Max block chunk read at a time
        """

        super().__init__(
            pairs=[p.checksum_free_address for p in pairs],
            oracles=oracles,
            reorg_mon=reorg_mon,
            timeframe=timeframe,
            data_retention_time=data_retention_time,
        )

        self.event_reader_context = LogContext()

        #: Pair address -> details mapping
        self.pair_map: Dict[str, PairDetails] = {p.address.lower(): p for p in pairs}
        self.web3_factory = web3_factory
        # A web3 instance used in the main thread
        self.web3 = web3_factory(self.event_reader_context)
        self.chunk_size = chunk_size
        self.max_threads = threads

        # Get data from ABI
        Pair = get_contract(self.web3, "sushi/UniswapV2Pair.json")
        self.events_to_read = [Pair.events.Swap, Pair.events.Sync]

    def get_pair_details(self, pair: str) -> PairDetails:
        return self.pair_map[pair.lower()]

    def get_all_pair_details(self) -> List[PairDetails]:
        return list(self.pair_map.values())

    def get_block_number_at_chain_tip(self) -> int:
        return self.web3.eth.block_number

    def fetch_trades(self,
                     start_block: int,
                     end_block: Optional[int],
                     tqdm: Optional[Type[tqdm]] = None) -> Iterable[Trade]:
        """Read data between logs.

        :raise ChainReorganisationDetected:
            In the case we notice chain data has changed during the reading
        """

        logger.debug("Fetching uniswap trades %d - %d", start_block, end_block)

        last_block = None

        max_blocks = end_block - start_block

        if tqdm:
            progress_bar = tqdm(total=max_blocks)
            progress_bar.set_description(f"Loading Uniswap v2 event data {start_block:,} - {end_block:,}, {len(self.pairs)} trading pairs")
        else:
            progress_bar = None

        # Listen only pairs we are interested in
        filter = Filter.create_filter(
            address=list(self.pair_map.keys()),
            event_types=self.events_to_read,
        )

        assert self.max_threads > 0

        if self.max_threads == 1:
            # Read in the current thread
            generator = read_events(
                self.web3,
                start_block,
                end_block,
                notify = None,
                chunk_size=self.chunk_size,
                context=self.event_reader_context,
                filter=filter,

            )
        else:
            # Read using a thread pool
            executor = create_thread_pool_executor(self.web3_factory, self.event_reader_context, max_workers=self.max_threads)
            generator = read_events_concurrent(
                 executor=executor,
                 start_block=start_block,
                 end_block=end_block,
                 notify=None,
                 chunk_size=self.chunk_size,
                 context=self.event_reader_context,
                 extract_timestamps=None,
                 reorg_mon=self.reorg_mon,
                 filter=filter,
             )

        sync = None

        # Read specified events in block range.
        # Sync() event should always come one log_index before Swap()
        events_processed = trades_processed = 0

        # Self sanity check that we don't create duplicates
        processed_swaps: Set[tuple] = set()

        for log_result in generator:
            events_processed += 1

            logger.debug("Reading %s event, block: %s, chunk: %d, log_index: %s, tx: %s",
                         log_result["event"].event_name,
                         convert_jsonrpc_value_to_int(log_result["blockNumber"]),
                         log_result["chunk_id"],
                         log_result["logIndex"],
                         log_result["transactionHash"])

            if log_result["event"].event_name == "Swap":
                swap = decode_swap(log_result)

                # Check that our read did not have duplicates
                swap_id = (swap["tx_hash"], swap["log_index"])
                assert swap_id not in processed_swaps, f"Tried to add swap twice: {swap}"
                processed_swaps.add(swap_id)

                trade = self.construct_trade_from_uniswap_v2_events(sync, swap)
                if trade:
                    trades_processed += 1
                    yield trade
            elif log_result["event"].event_name == "Sync":
                sync = decode_sync(log_result)
            else:
                raise RuntimeError(f"Cannot handle: {log_result}")

            if progress_bar:
                # Update progress bar for any block range we have processed.
                # Usually 1 but can be several if there has blocks without trades
                if last_block != log_result["blockNumber"]:
                    if last_block:
                        diff = log_result["blockNumber"] - last_block
                    else:
                        diff = 0
                    last_block = log_result["blockNumber"]
                    progress_bar.set_postfix({
                        "events": events_processed,
                        "trades": trades_processed,
                    }, refresh=False)
                    progress_bar.update(diff)

        logger.debug("Mapped %d events, %d trades", events_processed, trades_processed)

        if progress_bar:
            progress_bar.close()

    def construct_trade_from_uniswap_v2_events(self, prev_sync: Optional[dict], swap: dict) -> Optional[Trade]:
        """Figure out Uniswap v2 swap and volume.

        This is a stateful mapping: we need to be able
        to access previous Pair events to correctly deduct the price.

        :param prev_sync:
            The previous Sync() event that defines the price for this swap.
        """

        if prev_sync is None:
            logger.debug("Could not match Sync() for Swap(): %s", swap)
            return None

        # Swap
        tx_hash = swap["tx_hash"]
        if prev_sync["tx_hash"] != tx_hash:
            logger.debug("Current sync and swap do not follow Uniswap logic: %s - %s", prev_sync, swap)
            return None

        pair: PairDetails
        swap_pair_address = swap["pair_contract_address"].lower()
        pair = self.pair_map.get(swap_pair_address)
        assert pair is not None, f"Pair {swap_pair_address} not in our pair map {self.pair_map.keys()}"

        oracle: BasePriceOracle = self.oracles.get(pair.checksum_free_address)
        if not oracle:
            raise RuntimeError(f"Exchange rate oracle missing for pair %s", pair)

        exchange_rate = oracle.calculate_price(swap["block_number"])

        reserve0 = pair.token0.convert_to_decimals(prev_sync["reserve0"])
        reserve1 = pair.token1.convert_to_decimals(prev_sync["reserve1"])
        amount0_in = pair.token0.convert_to_decimals(swap["amount0_in"])
        amount1_in = pair.token1.convert_to_decimals(swap["amount1_in"])
        amount0_out = pair.token0.convert_to_decimals(swap["amount0_out"])
        amount1_out = pair.token1.convert_to_decimals(swap["amount1_out"])

        kind, price, amount = calculate_reserve_price_in_quote_token_decimal(
            pair.reverse_token_order,
            reserve0,
            reserve1,
            amount0_in,
            amount1_in,
            amount0_out,
            amount1_out
        )

        if kind == SwapKind.invalid:
            # Example trade:
            #
            # Has only amount1_in and amount1_out, probably
            # crafted by a buggy low level bot / aggregator
            #
            # {'type': 'swap', 'block_number': 38101777, 'block_hash': '0x8380f28ebb2ad49337631d688c213bd3fe398d8e1417759c1c9c9b3e6a57baa5', 'timestamp': 1673770206, 'tx_hash': '0x90cd4e551e917c101638ac93068325468a6cfba965c607a179f5b1d74c018f98', 'log_index': 108, 'pair_contract_address': '0x6e7a5fafcec6bb1e78bae2a1f0b612012bf14827', 'amount0_in': 0, 'amount1_in': 8707155, 'amount0_out': 0, 'amount1_out': 8681033}
            logger.debug("Could not determine trade: %s", swap)
            return None

        timestamp = self.reorg_mon.get_block_timestamp(swap["block_number"])

        # Flip for Trade() object
        if kind == SwapKind.sell:
            amount = -amount

        t = Trade(
            pair=pair.address.lower(),
            block_number=swap["block_number"],
            block_hash=swap["block_hash"],
            log_index=swap["log_index"],
            tx_hash=swap["tx_hash"],
            timestamp=pd.Timestamp.utcfromtimestamp(timestamp),
            price=price,
            amount=amount,
            exchange_rate=exchange_rate,
        )
        logger.debug("Uniswap trade processed: %s", t)
        return t


def decode_swap(log: LogResult) -> dict:
    """Process swap event.

    This function does manually optimised high speed decoding of the event.

    The event signature is:

    .. code-block::

        event Swap(
          address indexed sender,
          uint amount0In,
          uint amount1In,
          uint amount0Out,
          uint amount1Out,
          address indexed to
        );
    """

    # Raw example event
    # {'address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'blockHash': '0x4ba33a650f9e3d8430f94b61a382e60490ec7a06c2f4441ecf225858ec748b78', 'blockNumber': '0x98b7f6', 'data': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000046ec814a2e900000000000000000000000000000000000000000000000000000000000003e80000000000000000000000000000000000000000000000000000000000000000', 'logIndex': '0x4', 'removed': False, 'topics': ['0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822', '0x000000000000000000000000f164fc0ec4e93095b804a4795bbe1e041497b92a', '0x0000000000000000000000008688a84fcfd84d8f78020d0fc0b35987cc58911f'], 'transactionHash': '0x932cb88306450d481a0e43365a3ed832625b68f036e9887684ef6da594891366', 'transactionIndex': '0x1', 'context': <__main__.TokenCache object at 0x104ab7e20>, 'event': <class 'web3._utils.datatypes.Swap'>, 'timestamp': 1588712972}

    pair_contract_address = log["address"]

    # Chop data blob to byte32 entries
    data_entries = decode_data(log["data"])

    amount0_in, amount1_in, amount0_out, amount1_out = data_entries

    data = {
        "type": "swap",
        "block_number": convert_jsonrpc_value_to_int(log["blockNumber"]),
        "block_hash": log["blockHash"],
        "timestamp": log["timestamp"],
        "tx_hash": log["transactionHash"],
        "log_index": convert_jsonrpc_value_to_int(log["logIndex"]),
        "pair_contract_address": pair_contract_address,
        "amount0_in": convert_int256_bytes_to_int(amount0_in),
        "amount1_in": convert_int256_bytes_to_int(amount1_in),
        "amount0_out": convert_int256_bytes_to_int(amount0_out),
        "amount1_out": convert_int256_bytes_to_int(amount1_out),
    }
    return data


def decode_sync(log: LogResult) -> dict:
    """Process sync event.

    This function does manually optimised high speed decoding of the event.

    The event signature is:

    .. code-block::

        event Sync(uint112 reserve0, uint112 reserve1);
    """

    # Raw example event
    # {'address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'blockHash': '0x4ba33a650f9e3d8430f94b61a382e60490ec7a06c2f4441ecf225858ec748b78', 'blockNumber': '0x98b7f6', 'data': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000046ec814a2e900000000000000000000000000000000000000000000000000000000000003e80000000000000000000000000000000000000000000000000000000000000000', 'logIndex': '0x4', 'removed': False, 'topics': ['0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822', '0x000000000000000000000000f164fc0ec4e93095b804a4795bbe1e041497b92a', '0x0000000000000000000000008688a84fcfd84d8f78020d0fc0b35987cc58911f'], 'transactionHash': '0x932cb88306450d481a0e43365a3ed832625b68f036e9887684ef6da594891366', 'transactionIndex': '0x1', 'context': <__main__.TokenCache object at 0x104ab7e20>, 'event': <class 'web3._utils.datatypes.Swap'>, 'timestamp': 1588712972}

    pair_contract_address = log["address"]

    # Chop data blob to byte32 entries
    data_entries = decode_data(log["data"])

    reserve0, reserve1 = data_entries

    data = {
        "type": "sync",
        "block_number": convert_jsonrpc_value_to_int(log["blockNumber"]),
        "block_hash": log["blockHash"],
        "timestamp": log["timestamp"],
        "tx_hash": log["transactionHash"],
        "log_index": convert_jsonrpc_value_to_int(log["logIndex"]),
        "pair_contract_address": pair_contract_address,
        "reserve0": convert_int256_bytes_to_int(reserve0),
        "reserve1": convert_int256_bytes_to_int(reserve1),
    }
    return data


def calculate_reserve_price_in_quote_token_decimal(
        reversed: bool,
        reserve0: Decimal,
        reserve1: Decimal,
        amount0_in: Decimal,
        amount1_in: Decimal,
        amount0_out: Decimal,
        amount1_out: Decimal,
) -> Tuple[SwapKind, Decimal, Decimal]:
    """Calculate the market price based on Uniswap pool reserve0 an reserve1.

    All inputs are converted from fixed point numbers
    to natural decimal point placed numbers.

    :param reversed:
        Determine base, quote token order relative to token0, token1.
        If reversed, quote token is token0, else quote token is token0.

    :return:
        Price in quote token, amount in quote token
    """

    assert reserve0 > 0, f"Bad reserves {reserve0}, {reserve1}"
    assert reserve1 > 0, f"Bad reserves {reserve0}, {reserve1}"

    # One of those funny txs...
    if amount0_in == amount0_out:
        return SwapKind.invalid, Decimal(0), Decimal(0)

    if reversed:
        reserve0, reserve1 = reserve1, reserve0

    if reversed:
        quote_amount = (amount0_out - amount0_in)
        base_amount = (amount1_out - amount1_in)
    else:
        base_amount = (amount0_out - amount0_in)
        quote_amount = (amount1_out - amount1_in)

    if quote_amount == 0 or base_amount == 0:
        return SwapKind.invalid, Decimal(0), Decimal(0)

    price = reserve1 /reserve0

    # filter out broken swap event like: https://bscscan.com/tx/0x3156a93cd96ac0a1f5c6bfc99850ce6e78fc25a7c756f27a47d02114d8348c47
    if price <= 0:
        return SwapKind.invalid, Decimal(0), Decimal(0)

    # Quote token (fiat currency) increases
    if quote_amount > 0:
        kind = SwapKind.sell
        volume = quote_amount
    else:
        kind = SwapKind.buy
        volume = abs(quote_amount)

    return kind, price, volume
