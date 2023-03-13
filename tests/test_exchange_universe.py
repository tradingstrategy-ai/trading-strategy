from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client


def test_look_up_by_factory(persistent_test_client: Client):
    """Look up a Uniswap v2 compatible exchange by its factory."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    pancakewap_v2 = exchange_universe.get_by_chain_and_factory(ChainId.bsc, "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73")
    assert pancakewap_v2, "No pancakes :("
    assert pancakewap_v2.name == "PancakeSwap v2"


def test_look_up_by_chain_and_name(persistent_test_client: Client):
    """Look up a Uniswap v2 compatible exchange by its slug."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    
    quickswap = exchange_universe.get_by_chain_and_name(ChainId.polygon, "Quickswap")
    assert quickswap
    assert quickswap.exchange_slug == "quickswap"

    # there should be only 1 quickswap on Polygon
    nonexist = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "Quickswap")
    assert nonexist is None



def test_look_up_by_chain_and_slug(persistent_test_client: Client):
    """Look up a Uniswap v2 compatible exchange by its slug."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    
    quickswap = exchange_universe.get_by_chain_and_slug(ChainId.polygon, "quickswap")
    assert quickswap
    assert quickswap.name == "Quickswap"

    # there should be only 1 quickswap on Polygon
    nonexist = exchange_universe.get_by_chain_and_slug(ChainId.ethereum, "quickswap")
    assert nonexist is None
