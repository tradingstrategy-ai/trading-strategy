"""Cached data store for trade feeds.

We store

- Block headers

- Fetched trades

We do not store

- Candles (always regenerated)
"""
import logging
from pathlib import Path

from eth_defi.event_reader.block_header import BlockHeader
from eth_defi.event_reader.parquet_block_data_store import ParquetDatasetBlockDataStore
from tradingstrategy.direct_feed.trade_feed import TradeFeed


logger = logging.getLogger(__name__)


def save_trade_feed(trade_feed: TradeFeed, base_path: Path, partition_size: int):
    """Save the trade and block header data.

    :param trade_feed:
        Save trades and block headers from this feed.

    :param base_path:
        Base folder where data is dumped.
        Both headers and trades get their own Parquet datasets
        as folders.

    :parma partition_size:
        Partition size for the store.

    :return:
    """
    header_store = ParquetDatasetBlockDataStore(Path(base_path).joinpath("blocks"), partition_size)
    trade_store = ParquetDatasetBlockDataStore(Path(base_path).joinpath("trades"), partition_size)

    # Save headers
    headers_df = trade_feed.reorg_mon.to_pandas(partition_size)
    if len(headers_df) > 0:
        header_store.save(headers_df)
        assert not header_store.is_virgin(), f"Headers not correctly written"

    # Save trades
    trades_df = trade_feed.to_pandas(partition_size)
    if len(trades_df) > 0:
        trade_store.save(trades_df, check_contains_all_blocks=False)
        assert not trade_store.is_virgin(), f"Trades not correctly written"


def load_trade_feed(trade_feed: TradeFeed, base_path: Path, partition_size: int) -> bool:
    """Load trade and block header data.

    :param trade_feed:
        Save trades and block headers from this feed.

    :param base_path:
        Base folder where data is dumped.
        Both headers and trades get their own Parquet datasets
        as folders.

    :parma partition_size:
        Partition size for the store.

    :return:
        True if any data was loaded.

    """
    header_store = ParquetDatasetBlockDataStore(Path(base_path).joinpath("blocks"), partition_size)
    trade_store = ParquetDatasetBlockDataStore(Path(base_path).joinpath("trades"), partition_size)

    if not header_store.is_virgin():
        logger.info("Loading block header data from %s", header_store.path)
        headers_df_2 = header_store.load()
        block_map = BlockHeader.from_pandas(headers_df_2)
        trade_feed.reorg_mon.restore(block_map)
        logger.info("Loaded %d blocks", len(block_map))

    if not trade_store.is_virgin():
        trades_df_2 = trade_store.load()
        trade_feed.restore(trades_df_2)

    return not(header_store.is_virgin() or trade_store.is_virgin())
