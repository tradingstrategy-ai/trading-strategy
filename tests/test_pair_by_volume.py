"""Correctly pick pair by volume."""

import pytest
from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.pair import PandasPairUniverse, DEXPair, DuplicatePair


# TODO: Needs rewrite
@pytest.mark.skip(reason="Since 0.3 fake tokens are mostly filtered on the server side and thus there is no fake AAVE token in the dataset")
def test_one_pair_by_volume(logger, persistent_test_client: Client):
    """If we have multiple fake trading pairs, we pick the correct one by volume."""

    capitalgram = persistent_test_client

    exchange_universe = capitalgram.fetch_exchange_universe()

    # Fetch all trading pairs across all exchanges
    columnar_pair_table = capitalgram.fetch_pair_universe()
    pair_universe = PandasPairUniverse(columnar_pair_table.to_pandas())

    uniswap = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "uniswap v2")

    # AAVE-WETH has fake trading pairs
    with pytest.raises(DuplicatePair):
        pair_universe.get_one_pair_from_pandas_universe(
            uniswap.exchange_id,
            "AAVE",
            "WETH")

    # AAVE-WETH has fake trading pairs
    pair1: DEXPair = pair_universe.get_one_pair_from_pandas_universe(
        uniswap.exchange_id,
        "AAVE",
        "WETH",
        pick_by_highest_vol=True)

    assert pair1.buy_volume_all_time > 400_000


def test_dataset_create_single_pair_universe_conflict(logger, persistent_test_client: Client):
    """Get an exception when encountering a duplicate symbol on create_single_pair_universe.

    """

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    columnar_pair_table = client.fetch_pair_universe()
    pairs_df = columnar_pair_table.to_pandas()

    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")

    # 100k duplicates...
    with pytest.raises(DuplicatePair):
        PandasPairUniverse.create_single_pair_universe(
            pairs_df,
            exchange,
            "WBNB",
            "BUSD",
            pick_by_highest_vol=False,
        )


def test_dataset_create_single_pair_universe_resolve_by_volume(logger, persistent_test_client: Client):
    """Resolve duplicate conflicts by volume"""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    columnar_pair_table = client.fetch_pair_universe()
    pairs_df = columnar_pair_table.to_pandas()

    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")

    pair_universe = PandasPairUniverse.create_single_pair_universe(
            pairs_df,
            exchange,
            "WBNB",
            "BUSD",
            pick_by_highest_vol=True,
        )

    pair = pair_universe.get_single()
    assert pair.base_token_symbol == "WBNB"
    assert pair.quote_token_symbol == "BUSD"


