"""Test fixtures."""
import logging
import os
import sys

import pandas as pd
import pytest

from tradingstrategy.client import Client
from tradingstrategy.exchange import ExchangeUniverse
from tradingstrategy.pair import PandasPairUniverse


@pytest.fixture(scope="session")
def client() -> Client:
    """Create a client that uses test API key from OS environment against the production server.

    This fixture has the duration of the whole test session: once downloaded data is cached and is not redownloaded.
    """
    client = Client.create_test_client()
    yield client
    client.close()


@pytest.fixture(scope="session")
def cache_path(client: Client):
    cache_path = client.transport.cache_path
    return cache_path


@pytest.fixture(scope="session")
def persistent_test_client() -> Client:
    """Create a client that never redownloads data in a local dev env."""

    # Use persistent cache across reboots - because tests download a lot of data
    path = os.path.expanduser("~/.cache/trading-strategy-tests")
    c = Client.create_test_client(path)

    # Old testing hack
    if os.environ.get("CLEAR_CACHES"):
        c.clear_caches()

    yield c
    c.close()


@pytest.fixture(scope="session")
def logger(request) -> logging.Logger:
    """Initialize stdout logger using colored output."""

    logger = logging.getLogger()

    # pytest --log-level option
    log_level = request.config.getoption("--log-level")

    # Set log format to dislay the logger name to hunt down verbose logging modules
    fmt = "%(name)-25s %(levelname)-8s %(message)s"

    # Use colored logging output for console
    try:
        import coloredlogs
        coloredlogs.install(level=log_level, fmt=fmt, logger=logger)
    except ImportError:
        logging.basicConfig(stream=sys.stdout, level=log_level)

    # Disable logging of JSON-RPC requests and reploes
    logging.getLogger("web3.RequestManager").setLevel(logging.WARNING)
    logging.getLogger("web3.providers.HTTPProvider").setLevel(logging.WARNING)

    # Disable all internal debug logging of requests and urllib3
    # E.g. HTTP traffic
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # IPython notebook internal
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    return logger


@pytest.fixture(scope='session')
def default_exchange_universe(persistent_test_client: Client) -> ExchangeUniverse:
    """Load and construct exchange universe from the live server"""
    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    return exchange_universe


@pytest.fixture(scope='session')
def default_pairs_df(persistent_test_client: Client) -> pd.DataFrame:
    """Load pairs data from the live server"""
    client = persistent_test_client
    raw_pairs = client.fetch_pair_universe().to_pandas()
    return raw_pairs


@pytest.fixture(scope='session')
def default_pair_universe(
    persistent_test_client: Client,
    default_exchange_universe: ExchangeUniverse,
    default_pairs_df,
) -> PandasPairUniverse:
    """Load and construct pair universe from the live server.

    - Include and index all pairs
    """
    raw_pairs = default_pairs_df
    pair_universe = PandasPairUniverse(raw_pairs, build_index=True, exchange_universe=default_exchange_universe)
    return pair_universe

