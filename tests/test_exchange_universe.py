from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client


def test_look_up_by_factory(persistent_test_client: Client):
    """Look up a Uniswap v2 compatible exchange by its factory."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()
    pancakewap_v2 = exchange_universe.get_by_chain_and_factory(ChainId.bsc, "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73")
    assert pancakewap_v2, "No pancakes :("
    assert pancakewap_v2.name == "PancakeSwap v2"

