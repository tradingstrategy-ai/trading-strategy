import os

from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.client import Capitalgram
from tradingstrategy.chain import ChainId
from tradingstrategy.pair import PairUniverse


def test_client_ping(client: Capitalgram):
    """Unauthenticated ping"""
    data = client.transport.ping()
    assert data["ping"] == "pong"


def test_client_motd(client: Capitalgram):
    """Authenticated ping"""
    data = client.transport.message_of_the_day()
    assert "version" in data
    assert "message" in data


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
    pairs = client.fetch_pair_universe()
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/pair-universe.parquet")
    # Check universe has data
    assert len(pairs) > 0

    # The first ever pair in Uniswap v2
    # ETH-USDC
    universe = PairUniverse.create_from_pyarrow_table(pairs)
    first_pair = universe.pairs[1]
    assert first_pair.base_token_symbol == "WETH"
    assert first_pair.quote_token_symbol == "USDC"

    # The total pair count on Ethereum is quite high
    assert len(universe.pairs) > 40_000


def test_client_download_exchange_universe(client: Capitalgram, cache_path: str):
    """Download exchange mapping data"""
    universe = client.fetch_exchange_universe()
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/exchange-universe.json")
    # Check universe has data
    assert len(universe.exchanges) > 0
    assert universe.exchanges[1].name == "Uniswap v2"
    assert universe.exchanges[1].address == "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"
    assert universe.exchanges[22].name == "Sushiswap"
    assert universe.exchanges[22].address == "0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac"
    assert universe.exchanges[225].name == "Shiba Swap"


def test_client_download_all_pairs(client: Capitalgram, cache_path: str):
    """Download all candles for a specific candle width."""
    df = client.fetch_all_candles(TimeBucket.d30)
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/candles-30d.parquet")
    assert len(df) > 100


def test_client_download_all_liquidity_samples(client: Capitalgram, cache_path: str):
    """Download all liquidity samples for a specific candle width."""
    df = client.fetch_all_liquidity_samples(TimeBucket.d30)
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/liquidity-samples-30d.parquet")
    assert len(df) > 100


def test_client_convert_all_pairs_to_pandas(client: Capitalgram, cache_path: str):
    """We can convert the columnar Pyarrow data to Pandas format.

    This has some issues with timestamps, so adding a test.
    """
    pairs_table = client.fetch_pair_universe()
    df = pairs_table.to_pandas()
    assert len(df) > 1000
