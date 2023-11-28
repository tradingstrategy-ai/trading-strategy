import pytest
from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.exchange import ExchangeNotFoundError


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

    with pytest.raises(ExchangeNotFoundError) as excinfo:
        # there should be only 1 quickswap on Polygon
        nonexist = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "Quickswap")
        assert nonexist is None
    
    assert excinfo.value.args[0] == 'The trading universe does not contain exchange data on chain ethereum for exchange_name quickswap. This might be a problem in your data loading and filtering. \n                \n    Use tradingstrategy.ai website to explore DEXs.\n    \n    Here is a list of DEXes: https://tradingstrategy.ai/trading-view/exchanges\n    \n    For any further questions join our Discord: https://tradingstrategy.ai/community'



def test_look_up_by_chain_and_slug(persistent_test_client: Client):
    """Look up a Uniswap v2 compatible exchange by its slug."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    
    quickswap = exchange_universe.get_by_chain_and_slug(ChainId.polygon, "quickswap")
    assert quickswap
    assert quickswap.name == "Quickswap"

    with pytest.raises(ExchangeNotFoundError) as excinfo:
        # there should be only 1 quickswap on Polygon
        nonexist = exchange_universe.get_by_chain_and_slug(ChainId.ethereum, "quickswap")
        assert nonexist is None
    
    assert excinfo.value.args[0] == 'The trading universe does not contain exchange data on chain ethereum for exchange_slug quickswap. This might be a problem in your data loading and filtering. \n                \n    Use tradingstrategy.ai website to explore DEXs.\n    \n    Here is a list of DEXes: https://tradingstrategy.ai/trading-view/exchanges\n    \n    For any further questions join our Discord: https://tradingstrategy.ai/community'
