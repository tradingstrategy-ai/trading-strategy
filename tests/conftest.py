"""Test fixtures."""
import logging
import os
import sys

import pytest

from tradingstrategy.client import Client


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
    """Create a client that never redownloads data in a local dev env.
    """
    c = Client.create_test_client("/tmp/trading-strategy-tests")

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
