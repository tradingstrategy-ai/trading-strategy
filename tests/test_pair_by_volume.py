"""Correctly pick pair by volume."""

import pytest
from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.pair import PandasPairUniverse, DEXPair, DuplicatePair


def test_pair_by_volume(logger, persistent_test_client: Client):
    """If we have multiple fake trading pairs, we pick the correct one by volume."""

    capitalgram = persistent_test_client

    exchange_universe = capitalgram.fetch_exchange_universe()

    # Fetch all trading pairs across all exchanges
    columnar_pair_table = capitalgram.fetch_pair_universe()
    pair_universe = PandasPairUniverse(columnar_pair_table.to_pandas())

    uniswap = exchange_universe.get_by_name_and_chain(ChainId.ethereum, "uniswap v2")

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