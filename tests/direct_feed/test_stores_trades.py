"""Cache past trades on disk."""
import tempfile


from eth_defi.price_oracle.oracle import TrustedStablecoinOracle
from tradingstrategy.direct_feed.reorg_mon import MockChainAndReorganisationMonitor
from tradingstrategy.direct_feed.store import save_trade_feed, load_trade_feed
from tradingstrategy.direct_feed.synthetic_feed import SyntheticTradeFeed


def test_save_load_block_headers_and_trades():
    """Save and load all direct feed data from the disk."""

    partition_size = 10

    with tempfile.TemporaryDirectory() as tmp_dir:

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

        save_trade_feed(feed, tmp_dir, partition_size)

        mock_chain_2 = MockChainAndReorganisationMonitor()

        feed2 = SyntheticTradeFeed(
            ["ETH-USD"],
            {"ETH-USD": TrustedStablecoinOracle()},
            mock_chain,
        )

        assert load_trade_feed(feed2, tmp_dir, partition_size) == True
        mock_chain_2.restore(feed2.reorg_mon.block_map)  # Hack needed to restore the simulated chain

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
