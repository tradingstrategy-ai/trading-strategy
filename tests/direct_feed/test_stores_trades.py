"""Cache past trades on disk."""
import tempfile
from pathlib import Path

from eth_defi.event_reader.parquet_block_data_store import ParquetDatasetBlockDataStore
from eth_defi.price_oracle.oracle import TrustedStablecoinOracle
from tradingstrategy.direct_feed.reorg_mon import MockChainAndReorganisationMonitor, BlockRecord
from tradingstrategy.direct_feed.synthetic_feed import SyntheticTradeFeed


def test_save_load_block_headers_and_trades():
    """Save and load all direct feed data from the disk."""

    partition_size = 10

    with tempfile.TemporaryDirectory() as tmp_dir, tempfile.TemporaryDirectory() as tmp_dir_2:
        header_store = ParquetDatasetBlockDataStore(Path(tmp_dir), partition_size)
        trade_store = ParquetDatasetBlockDataStore(Path(tmp_dir_2), partition_size)

        mock_chain = MockChainAndReorganisationMonitor()
        feed = SyntheticTradeFeed(
            ["ETH-USD"],
            {"ETH-USD": TrustedStablecoinOracle()},
            mock_chain,
        )
        mock_chain.produce_blocks(100)
        assert mock_chain.get_last_block_live() == 100
        assert feed.get_block_number_of_last_trade() == 100
        delta = feed.backfill_buffer(100, None)

        # Save headers
        headers_df = feed.reorg_mon.to_pandas(partition_size)
        header_store.save(headers_df)

        # Save trades
        trades_df = feed.to_pandas(partition_size)
        trade_store.save(trades_df)

        #
        # Now reload everything from the scracth
        #

        header_store = ParquetDatasetBlockDataStore(Path(tmp_dir), partition_size)
        trade_store = ParquetDatasetBlockDataStore(Path(tmp_dir_2), partition_size)

        mock_chain_2 = MockChainAndReorganisationMonitor()

        feed2 = SyntheticTradeFeed(
            ["ETH-USD"],
            {"ETH-USD": TrustedStablecoinOracle()},
            mock_chain,
        )

        headers_df_2 = header_store.load()
        trades_df_2 = trade_store.load()

        feed.reorg_mon.restore(headers_df_2)
        mock_chain_2.restore(headers_df_2.block_map)

        feed2.restore(trades_df_2)

        # Check state looks correctly restored
        assert mock_chain.get_last_block_live() == 100
        assert feed2.get_block_number_of_last_trade() == 100

        mock_chain.produce_blocks(1)
        delta = feed2.perform_duty_cycle()
        assert delta.start_block == 101
        assert delta.end_block == 101
        assert not delta.reorg_detected

        assert mock_chain.get_last_block_live() == 100
        assert feed2.get_block_number_of_last_trade() == 100
