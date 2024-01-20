"""Individual DEX trade data."""
import datetime
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Collection

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc

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

    @staticmethod
    def get_partitioning() -> :
        part = ds.partitioning(
            pa.schema([("c", pa.int16())]), flavor="hive"
        )
        return


def _extract_chain_trades(source: Path, target: Path):
    """Read all trades of the chain to a memory, sort them out."""

    chain_table = pa.table(schema=Trade.to_pyarrow_schema())


def split_trades_parquet(
    source: Path,
    destination_folder: Path,
    chains: Collection[ChainId] = {ChainId.arbitrum, ChainId.polygon, ChainId.bsc, ChainId.ethereum, ChainId.avalanche},
):
    """Preprocess the large monolithic trades dataset to more useable parts.

    - The monolithic Parquet file is ~100 GB, and most computers cannot process this in RAM.

    - Create a Parquet dataset that is sharded by a chain and more processable
    """

    assert source.is_file()
    assert destination_folder.is_dir(), f"Not a direction: {destination_folder}"

    for chain in chains:
        destination = destination_folder / f"trades-{chain.get_slug()}.parquet"
        _extract_chain_trades(source, destination)


def extract_unit_test_sample(
    source: Path,
    destination: Path,
    max_batches_per_chain=8,
    chains: Collection[ChainId] = {ChainId.arbitrum, ChainId.polygon},
):
    """Extract some data from the large trades file for unit testing.

    Then the unit testing sample is then stored with the source tree.

    :param source:
        uniswap-trades.parquet.

        The large 100 GB version.

    :param destination:
        unit-test-uniswap-trades.parquet

        Few megabytes version.
    """

    batches_per_chain = Counter[ChainId]()
    out = pq.ParquetWriter(
        destination,
        compression="zstd"
    )
    inp = pq.ParquetFile(source)
    for batch in inp.iter_batches():
        matched = False
        for chain in chains:
            if pc.any(pc.equal(batch["chain_id"], chain.value)):
                if batches_per_chain[chain] < max_batches_per_chain:
                    matched = True
                    batches_per_chain[chain] += 1

        if matched:
            out.write(batch)

    out.close()









