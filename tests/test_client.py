import os
import tempfile

import pytest

from capitalgram.candle import CandleBucket
from capitalgram.client import Capitalgram
from capitalgram.chain import ChainId


@pytest.fixture(scope="module")
def cache_path():
    cache_path = tempfile.mkdtemp()
    return cache_path


@pytest.fixture(scope="module")
def client(cache_path):
    c = Capitalgram.create_jupyter_client(cache_path=cache_path)
    return c


def test_client_fetch_chain_status(client: Capitalgram):
    """Get chain scanning status"""
    status = client.fetch_chain_status(ChainId.ethereum)
    assert status["chain_id"] == 1
    assert status["pairs"] > 0
    assert status["swaps"] > 0
    assert status["minute_candles"] > 0
    assert status["first_swap_at"] == '2020-05-05T21:09:32'


def test_client_download_pair_universe(client: Capitalgram, cache_path: str):
    """Download pair mapping data"""
    universe = client.fetch_pair_universe()
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/pair-universe.json.zstd")
    # Check universe has data
    assert len(universe.pairs) > 0
    # The first ever pair in Uniswap v2
    # ETH-USDC
    pair = universe.get_pair_by_id(1)
    assert pair.base_token_symbol == "WETH"
    assert pair.quote_token_symbol == "USDC"


def test_client_download_exchange_universe(client: Capitalgram, cache_path: str):
    """Download exchange mapping data"""
    universe = client.fetch_exchange_universe()
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/exchange-universe.json")
    # Check universe has data
    assert len(universe.exchanges) > 0
    assert universe.exchanges[1].name == "Uniswap v2"


def test_client_download_all_pairs(client: Capitalgram, cache_path: str):
    """Download all candles for a specific candle width."""
    df = client.fetch_all_candles(CandleBucket.d30)
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/candles-30d.feather")
    assert len(df) > 100


