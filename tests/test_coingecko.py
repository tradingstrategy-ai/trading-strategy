import os
from pprint import pprint

import pytest

from tradingstrategy.alternative_data.coingecko import CoingeckoUniverse, categorise_pairs, CoingeckoClient
from tradingstrategy.chain import ChainId


def test_category_pairs(persistent_test_client):
    """Chck that we can produce category metadata for downloaded trading pairs."""
    client = persistent_test_client
    pairs_df = client.fetch_pair_universe().to_pandas()
    coingecko_universe = CoingeckoUniverse.load()
    category_df = categorise_pairs(coingecko_universe, pairs_df)

    # Get pair_ids for all tokens in Coingecko's DeFi category
    mask = category_df["category"] == "Decentralized Finance (DeFi)"
    pair_ids = category_df[mask]["pair_id"]
    assert len(pair_ids) > 100


@pytest.mark.skipif(os.environ.get("COINGECKO_API_KEY") is None, reason="COINGECKO_API_KEY env needed")
def test_coingecko_fetch_token():
    """Chck that we can produce category metadata for downloaded trading pairs."""

    coingecko_client = CoingeckoClient(os.environ["COINGECKO_API_KEY"], demo=True)
    data = coingecko_client.fetch_by_contract(
        chain_id=ChainId.ethereum,
        contract_address="0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
    )
    assert data["web_slug"] == "aave"

    data = coingecko_client.fetch_by_contract(
        chain_id=ChainId.binance,
        contract_address="0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
    )
    assert data["web_slug"] == "wbnb"
