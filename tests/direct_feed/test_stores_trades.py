"""Cache past trades on disk."""
import tempfile
from pathlib import Path

from eth_defi.price_oracle.oracle import TrustedStablecoinOracle
from eth_defi.event_reader.reorganisation_monitor import MockChainAndReorganisationMonitor

from tradingstrategy.direct_feed.store import DirectFeedStore
from tradingstrategy.direct_feed.synthetic_feed import SyntheticTradeFeed


def test_save_load_block_headers_and_trades():
    """Save and load all direct feed data from the disk."""

    # Set some odd number to make it more likely
    # to surface qny issues
    partition_size = 27

    with tempfile.TemporaryDirectory() as tmp_dir:

        store = DirectFeedStore(Path(tmp_dir), partition_size)

        mock_chain = MockChainAndReorganisationMonitor()
        feed = SyntheticTradeFeed(
            ["ETH-USD"],
            {"ETH-USD": TrustedStablecoinOracle()},
            mock_chain,
        )
        mock_chain.produce_blocks(100)
        assert mock_chain.get_last_block_live() == 100
        delta = feed.backfill_buffer(100, None)
        assert feed.get_block_number_of_last_trade() == 100

        store.save_trade_feed(feed)

        mock_chain_2 = MockChainAndReorganisationMonitor()

        feed2 = SyntheticTradeFeed(
            ["ETH-USD"],
            {"ETH-USD": TrustedStablecoinOracle()},
            mock_chain,
        )

        assert store.load_trade_feed(feed2) == True
        mock_chain_2.restore(feed2.reorg_mon.block_map)  # Hack needed to restore the simulated chain

        # Check state looks correctly restored
        assert mock_chain.get_last_block_live() == 100
        assert feed2.get_block_number_of_last_trade() == 100

        mock_chain.produce_blocks(1)
        delta = feed2.perform_duty_cycle()
        assert delta.start_block == 60  # Snapped to candle boundary
        assert delta.end_block == 101
        assert not delta.reorg_detected

        assert mock_chain.get_last_block_live() == 101
        assert feed2.get_block_number_of_last_trade() == 101

        # Save again to ensure we do not have problems with repeatble writes
        store.save_trade_feed(feed)


def test_clear_store():
    """Save and load all direct feed data from the disk."""

    # Set some odd number to make it more likely
    # to surface qny issues
    partition_size = 27

    with tempfile.TemporaryDirectory() as tmp_dir:

        store = DirectFeedStore(Path(tmp_dir).joinpath("test_store"), partition_size)
        assert store.is_empty()

        mock_chain = MockChainAndReorganisationMonitor()
        feed = SyntheticTradeFeed(
            ["ETH-USD"],
            {"ETH-USD": TrustedStablecoinOracle()},
            mock_chain,
        )
        mock_chain.produce_blocks(100)
        assert mock_chain.get_last_block_live() == 100
        delta = feed.backfill_buffer(100, None)
        assert feed.get_block_number_of_last_trade() == 100

        store.save_trade_feed(feed)

        assert not store.is_empty()
        store.clear()
        assert store.is_empty()

