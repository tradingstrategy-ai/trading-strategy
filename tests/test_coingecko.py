from tradingstrategy.alternative_data.coingecko import CoingeckoUniverse, categorise_pairs


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

