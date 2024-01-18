"""Individual DEX trade data."""
import datetime
from dataclasses import dataclass

import pyarrow as pa

from tradingstrategy.chain import ChainId
from tradingstrategy.types import PrimaryKey, BlockNumber


@dataclass
class Trade:
    """Individual trade.

    Describe the data structure used for individual trades in DEX trades dataset.

    - Dataset contains all supported DEX trade across supported blockchains

    - Based on emitted Solidity events

    - Data can be cross-referenced to blockchain transactions and log indexed

    - Trades are Swaps for Uniswap

    - Dataset focuses on executed price, for market price feed
      see candle datasets

    - Trading pairs are normalised to base token - quote token
      human-readable format, as opposite to raw Uniswap token pairs

    - The approximate best effort USD exchange rate is included,
      when available, to calculate US dollar measured impact of trades

    - Dataset is sorted for the best file compression - trades
      are not guaranteed to be ordered by time
    """

    #: The blockchain where this swap happened
    chain_id: ChainId

    #: The trading pair internal id
    #:
    #: See :py:class:`tradingstrategy.pair.PandasPairUniverse` how to resolve
    #: internal ids to pair metadata.
    #:
    pair_id: PrimaryKey

    #: The block where this event happened
    #:
    block_number: BlockNumber

    #: The block production timestamp
    #:
    timestamp: datetime.datetime

    #: Transaction hash
    #:
    #: 32 bytes.
    #:
    tx_hash: bytes

    #: Swap event index within a block
    #:
    log_index: int

    #: How much quote token was traded in this trade.
    #:
    #: Quote token is identified from the :py:attr:`pair_id`
    #: metadata.
    #:
    quote_token_diff: float

    #: How much base token was traded in this trade.
    #:
    #: Base token is identified from the :py:attr:`pair_id`
    #: metadata.
    #:
    base_token_diff: float

    #: Quote token/USD exchange rate (if available).
    #:
    #: The approximate USD exchange rate to convert the trade
    #: value to dollars. Only available for some
    #: quoet tokens. It's an approximatino of a close
    #: time, so only suitable for statistical analysis.
    #:
    #: Set to zero if not available.
    #:
    usd_exchange_rate: float

    #: The trading pair/pool contract address.
    #:
    #: 160 bits, or 20 bytes.
    #:
    pool_address: bytes

    #: The transsaction originator address.
    sender_address: bytes

    #: The DEX internal id.
    #:
    #: Can be resolved with :py:class:`tradingstrategy.exchange.ExchangeUniverse`
    #:
    exchange_id: int

    @staticmethod
    def to_pyarrow_schema() -> pa.Schema:
        return pa.schema([
            ("chain_id", pa.uint32()),
            ("pair_id", pa.uint64()),
            ("block_number", pa.uint32()),
            ("timestamp", pa.timestamp("s")),
            ("tx_hash", pa.binary(32)),
            ("log_index", pa.uint32()),
            ("quote_token_diff", pa.float64()),
            ("base_token_diff", pa.float64()),
            ("usd_exchange_rate", pa.float64()),
            ("pool_address", pa.binary(20)),
            ("sender_address", pa.binary(20)),
            ("exchange_id", pa.uint32()),
        ])


