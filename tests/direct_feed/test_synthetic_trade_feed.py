from decimal import Decimal

import pandas as pd
import pytest
from tqdm import tqdm

from eth_defi.price_oracle.oracle import TrustedStablecoinOracle
from eth_defi.event_reader.reorganisation_monitor import MockChainAndReorganisationMonitor

from tradingstrategy.direct_feed.synthetic_feed import SyntheticTradeFeed
from tradingstrategy.direct_feed.trade_feed import Trade


def test_synthetic_block_mon_produce_blocks():
    """Create mocked blocks."""
    mock_reorg_mon = MockChainAndReorganisationMonitor()
    assert mock_reorg_mon.get_last_block_live() == 0
    assert mock_reorg_mon.get_last_block_read() == 0
    mock_reorg_mon.produce_blocks()


def test_synthetic_block_mon_find_reorgs():
    """There are never reorgs."""
    mock_reorg_mon = MockChainAndReorganisationMonitor()
    mock_reorg_mon.produce_blocks()
    mock_reorg_mon.figure_reorganisation_and_new_blocks()
    assert mock_reorg_mon.get_last_block_live() == 1
    assert mock_reorg_mon.get_last_block_read() == 1


def test_synthetic_block_mon_find_reorgs_100_blocks():
    """There are never reorgs in longer mock chain."""
    mock_reorg_mon = MockChainAndReorganisationMonitor()
    mock_reorg_mon.produce_blocks(100)
    mock_reorg_mon.figure_reorganisation_and_new_blocks()


def test_add_trades():
    """Add trades to thework buffer."""

    mock_chain = MockChainAndReorganisationMonitor()

    mock_chain.produce_blocks(1)

    feed = SyntheticTradeFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
    )

    assert len(feed.trades_df) == 0

    feed.add_trades([
        Trade(
            "ETH-USD",
            1,
            "0x1",
            pd.Timestamp.fromtimestamp(1, None),
            "0xff",
            1,
            Decimal(1),
            Decimal(1),
            Decimal(1),
        )
    ])

    assert len(feed.trades_df) == 1
    entry = feed.trades_df.iloc[0]
    assert entry["pair"] == "ETH-USD"

    assert feed.get_latest_price("ETH-USD") == Decimal(1)


def test_initial_load():
    """Read trades from a synthetic feed."""

    mock_chain = MockChainAndReorganisationMonitor()

    feed = SyntheticTradeFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
    )

    mock_chain.produce_blocks(100)
    assert len(mock_chain.simulated_blocks) == 100

    delta = feed.backfill_buffer(100, tqdm)

    assert delta.cycle == 1
    assert delta.start_block == 1
    assert delta.end_block == 100


def test_truncate():
    """Read trades from a synthetic feed."""

    mock_chain = MockChainAndReorganisationMonitor()

    feed = SyntheticTradeFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
    )

    mock_chain.produce_blocks(100)
    assert len(mock_chain.simulated_blocks) == 100

    feed.backfill_buffer(100, tqdm)
    assert len(feed.trades_df) == 508
    feed.truncate_reorganised_data(50)
    assert len(feed.trades_df) == 246


def test_initial_load_no_progress_bar():
    """Read trades from a synthetic feed, do not use progress br."""

    mock_chain = MockChainAndReorganisationMonitor()
    feed = SyntheticTradeFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
    )
    mock_chain.produce_blocks(100)
    delta = feed.backfill_buffer(100, None)
    assert delta.cycle == 1


def test_perform_cycle():
    """Iteratively read trades from the chain."""

    mock_chain = MockChainAndReorganisationMonitor()

    feed = SyntheticTradeFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
    )
    mock_chain.produce_blocks(100)
    assert mock_chain.get_last_block_live() == 100
    delta = feed.backfill_buffer(100, None)
    assert delta.start_block == 1
    assert delta.end_block == 100
    assert not delta.reorg_detected
    assert mock_chain.get_last_block_live() == 100
    assert mock_chain.get_last_block_read() == 100

    mock_chain.produce_blocks(2)
    assert mock_chain.get_last_block_live() == 102
    assert mock_chain.get_last_block_read() == 100

    delta = feed.perform_duty_cycle()
    assert mock_chain.get_last_block_live() == 102
    assert mock_chain.get_last_block_read() == 102
    assert delta.cycle == 2
    assert delta.start_block == 60
    assert delta.unadjusted_start_block == 101
    assert delta.end_block == 102
    assert not delta.reorg_detected


def test_perform_chain_reorg():
    """Simulate a chain reorganisation."""

    mock_chain = MockChainAndReorganisationMonitor(check_depth=100)

    feed = SyntheticTradeFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
    )
    mock_chain.produce_blocks(100)
    assert mock_chain.get_last_block_live() == 100
    delta = feed.backfill_buffer(100, None)
    assert delta.start_block == 1
    assert delta.end_block == 100
    assert not delta.reorg_detected

    # Trigger reorg by creating a changed block in the chain
    mock_chain.produce_fork(70)

    mock_chain.produce_blocks(2)
    assert mock_chain.get_last_block_live() == 102
    assert mock_chain.get_last_block_read() == 100

    # This will do 100 blocks deep reorg check
    delta = feed.perform_duty_cycle()
    assert mock_chain.get_last_block_live() == 102
    assert mock_chain.get_last_block_read() == 102
    assert delta.cycle == 2
    assert delta.start_block == 60
    assert delta.unadjusted_start_block == 70
    assert delta.end_block == 102
    assert delta.reorg_detected


def test_incremental():
    """Simulate incremental 1 block updates."""

    mock_chain = MockChainAndReorganisationMonitor(check_depth=100)

    feed = SyntheticTradeFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
    )
    mock_chain.produce_blocks(100)
    assert mock_chain.get_last_block_live() == 100
    delta = feed.backfill_buffer(100, None)
    feed.check_current_trades_for_duplicates()
    assert delta.start_block

    mock_chain.produce_blocks(1)
    feed.perform_duty_cycle()
    feed.check_current_trades_for_duplicates()

    mock_chain.produce_blocks(1)
    feed.perform_duty_cycle ()
    feed.check_current_trades_for_duplicates()

    mock_chain.produce_blocks(1)
    delta = feed.perform_duty_cycle()
    feed.check_current_trades_for_duplicates()

    assert delta.end_block == 103


def test_duplicate_trades():
    """Internal check for duplicate trades."""

    mock_chain = MockChainAndReorganisationMonitor(check_depth=100)

    feed = SyntheticTradeFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
    )
    mock_chain.produce_blocks(100)
    assert mock_chain.get_last_block_live() == 100
    feed.backfill_buffer(100, None)

    feed.check_current_trades_for_duplicates()

    # Manipulate feed
    feed.trades_df["tx_hash"] = "1"
    feed.trades_df["log_index"] = "1"

    with pytest.raises(AssertionError):
        feed.check_current_trades_for_duplicates()
