"""Client dataset download and integrity tests"""

import os
import logging
from pathlib import Path

from tradingstrategy.environment.jupyter import JupyterEnvironment
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.client import Client
from tradingstrategy.chain import ChainId

logger = logging.getLogger(__name__)


def test_client_ping(client: Client):
    """Unauthenticated ping"""
    data = client.transport.ping()
    assert data["ping"] == "pong"


def test_client_motd(client: Client):
    """Authenticated ping"""
    data = client.transport.message_of_the_day()
    assert "version" in data
    assert "message" in data


def test_client_fetch_chain_status(client: Client):
    """Get chain scanning status"""
    status = client.fetch_chain_status(ChainId.ethereum)
    assert status["chain_id"] == 1

    # TODO: The blockchain pair count is temporarily disabled for performance reasons,
    # and the chain data API just returns zero (0). Re-enable the assertion below once
    # the API is fixed.
    #
    # https://github.com/tradingstrategy-ai/oracle/commit/1dac3e1ef3c84ae7b6509242d114d4f6d1ae384a
    # assert status["pairs"] > 0

    # Removed as too slow to compute on the server-side for now
    # assert status["swaps"] > 0
    # assert status["minute_candles"] > 0
    # assert status["first_swap_at"] == '2020-05-05T21:09:32'


def test_client_download_exchange_universe(client: Client, cache_path: str):
    """Download exchange mapping data"""
    universe = client.fetch_exchange_universe()
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/exchange-universe.json")
    # Check universe has data
    assert len(universe.exchanges) > 0
    assert universe.exchanges[1].name == "Uniswap v2"
    assert universe.exchanges[1].exchange_slug == "uniswap-v2"
    assert universe.exchanges[1].chain_slug == "ethereum"
    assert universe.exchanges[1].address == "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"
    exchange = universe.get_by_chain_and_slug(ChainId.ethereum, "sushi")
    assert exchange.name == "Sushi"
    assert exchange.address == "0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac"
    exchange = universe.get_by_chain_and_name(ChainId.ethereum, "Shiba Swap")
    assert exchange.name == "Shiba Swap"
    assert exchange.exchange_slug == "shiba-swap"


def test_client_download_all_pairs(client: Client, cache_path: str):
    """Download all candles for a specific candle width."""
    df = client.fetch_all_candles(TimeBucket.d30)
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/candles-30d.parquet")
    assert len(df) > 100


def test_client_download_all_liquidity_samples(client: Client, cache_path: str):
    """Download all liquidity samples for a specific candle width."""
    df = client.fetch_all_liquidity_samples(TimeBucket.d30)
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/liquidity-samples-30d.parquet")
    assert len(df) > 100


def test_client_convert_all_pairs_to_pandas(client: Client, cache_path: str):
    """We can convert the columnar Pyarrow data to Pandas format.

    This has some issues with timestamps, so adding a test.
    """
    pairs_table = client.fetch_pair_universe()
    df = pairs_table.to_pandas()
    assert len(df) > 1000


# Not yet supported
#
#@pytest.mark.asyncio
#async def test_create_pyodide_client_indexdb():
#    """Test the special client used in Pyodide which use IndexDB to save the API key."""
#    # https://tonybaloney.github.io/posts/async-test-patterns-for-pytest-and-unittest.htmlpy
#    env = JupyterEnvironment()
#    env.clear_configuration()
#    client = await Client.create_pyodide_client_async(remember_key=False)
#    assert isinstance(client, Client)


def test_create_pyodide_client_detect():
    """Test the special client used in Pyodide which use HTTP referral authentication."""
    env = JupyterEnvironment()
    env.clear_configuration()
    client = Client.create_jupyter_client(pyodide=True)
    assert isinstance(client, Client)
