from tqdm import tqdm

from eth_defi.price_oracle.oracle import TrustedStablecoinOracle
from tradingstrategy.direct_feed.reorgmon import TestReorganisationMonitor
from tradingstrategy.direct_feed.synthetic_feed import SyntheticFeed


def test_initial_load():
    """Read trades from a synthetic feed."""

    mock_chain = TestReorganisationMonitor(1)

    feed = SyntheticFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
    )

    mock_chain.increment_block(100)

    delta = feed.backfill_buffer(100, tqdm)

    assert delta.cycle == 1
    assert delta.start_block == 1
    assert delta.end_block == 100


def test_initial_load_no_pogress_Bar():
    """Read trades from a synthetic feed, do not use progress br."""

    mock_chain = TestReorganisationMonitor(1)

    feed = SyntheticFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
    )
    mock_chain.increment_block(100)
    delta = feed.backfill_buffer(100, None)
    assert delta.cycle == 1


