"""Cached data store for trade feeds.

We store

- Block headers

- Fetched trades

We do not store

- Candles (always regenerated)
"""
import logging
import os
import shutil
from pathlib import Path
from typing import Tuple

from eth_defi.event_reader.block_header import BlockHeader
from eth_defi.event_reader.parquet_block_data_store import ParquetDatasetBlockDataStore
from tradingstrategy.direct_feed.trade_feed import TradeFeed


logger = logging.getLogger(__name__)


class DirectFeedStore:
    """Manage on-disk block header and trade cache for direct feeds.

    Internally uses partitioned Parquet dataset storage.
    Each partition is a range of blocks and goes to different folder/file.
    """

    def __init__(self, base_path: Path, partition_size: int):
        """Initialise a new store.

        :param base_path:
            Base folder where data is dumped.
            Both headers and trades get their own Parquet datasets
            as folders.

        :param partition_size:
            Partition size for the store.
            Expressed as number of blocks per parquet file.

        """
        assert isinstance(base_path, Path)
        assert type(partition_size) == int
        self.base_path = base_path
        self.partition_size = partition_size

    def is_empty(self) -> bool:
        """Have we written anything to this store yer."""
        return not self.base_path.exists()

    def clear(self):
        """Clear cache."""
        assert not self.is_empty(), f"Cannot clear empty store."
        if self.base_path.exists():
            shutil.rmtree(self.base_path)

    def save_trade_feed(self, trade_feed: TradeFeed) -> Tuple[int, int]:
        """Save the trade and block header data.

        :param trade_feed:
            Save trades and block headers from this feed.

        :return:
            Last saved header block number, last saved trade number
        """

        base_path = self.base_path
        partition_size = self.partition_size

        header_store = ParquetDatasetBlockDataStore(Path(base_path).joinpath("blocks"), partition_size)
        trade_store = ParquetDatasetBlockDataStore(Path(base_path).joinpath("trades"), partition_size)

        # Save headers
        headers_df = trade_feed.reorg_mon.to_pandas(partition_size)
        if len(headers_df) > 0:
            header_store.save(headers_df)
            assert not header_store.is_virgin(), f"Headers not correctly written"
            last_header_block = headers_df.iloc[-1]["block_number"]
        else:
            last_header_block = 0

        # Save trades
        trades_df = trade_feed.to_pandas(partition_size)
        if len(trades_df) > 0:
            trade_store.save(trades_df, check_contains_all_blocks=False)
            assert not trade_store.is_virgin(), f"Trades not correctly written"
            last_trade_block = trades_df.iloc[-1]["block_number"]
        else:
            last_trade_block = 0

        return last_header_block, last_trade_block

    def load_trade_feed(self, trade_feed: TradeFeed) -> bool:
        """Load trade and block header data.

        :param trade_feed:
            Save trades and block headers from this feed.

        :return:
            True if any data was loaded.
        """

        base_path = self.base_path
        partition_size = self.partition_size

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
            logger.info(f"Loaded {len(trades_df_2)}, last block is {trade_feed.get_block_number_of_last_trade():,}")

        return not(header_store.is_virgin() or trade_store.is_virgin())
