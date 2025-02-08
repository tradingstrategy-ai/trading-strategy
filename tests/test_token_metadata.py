"""CLMM data tests."""

import datetime
from pathlib import Path

import pytest
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.transport.cache import APIError
from tradingstrategy.utils.token_extra_data import load_token_metadata
from tradingstrategy.utils.token_filter import add_base_quote_address_columns, filter_for_stablecoins, StablecoinFilteringMode, filter_for_derivatives, filter_for_quote_tokens, deduplicate_pairs_by_volume


def test_load_token_metadata(
    persistent_test_client: Client,
):
    """Load CLMM data for two pairs on Uniswap v3."""

    client = persistent_test_client

    metadata = client.fetch_token_metadata(
        chain_id=ChainId.ethereum,
        # AAVe, USDC
        addresses={"0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"}
    )
    assert len(metadata) == 2

    # Loaded Aave
    assert "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9".lower() in metadata

    usdc = metadata["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"]
    print(usdc)


@pytest.mark.skip(reason="Server-side error messages must be fine-tuned")
def test_load_metadata_single_bad_token(
    persistent_test_client: Client,
):
    """"One of loaded tokens is unsupported"""
    client = persistent_test_client

    metadata = client.fetch_token_metadata(
        ChainId.ethereum,
        # AAVe, USDC
        {"0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", "0xFFFF6991c6218b36c1d19d4a2e9eb0ce3606eb48"}
    )
    assert len(metadata) == 1


@pytest.mark.skip(reason="Unfinished")
def test_create_trading_universe_with_token_metadata(
    persistent_test_client: Client,
    default_pair_universe,
):
    """Create a trading universe using /token-metadata endpoint for filtering scams"""
    client = persistent_test_client

    chain_id = ChainId.avalanche
    exchanges = {"trader-joe", "pangolin"}

    pairs_df = client.fetch_pair_universe().to_pandas()

    # Drop other chains to make the dataset smaller to work with
    chain_mask = pairs_df["chain_id"] == chain_id.value
    pairs_df = pairs_df[chain_mask]

    # Build subset of pairs we are going to use.
    # - Remove stable/stable airs
    # - Remove derivative tokens like staked ETH
    category_df = pairs_df
    category_df = add_base_quote_address_columns(category_df)
    category_df = filter_for_stablecoins(category_df, StablecoinFilteringMode.only_volatile_pairs)
    category_df = filter_for_derivatives(category_df)

    # Take pairs only in supported quote token
    allowed_quotes = {
        "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",  # USDC
        "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",  # WAVAX
    }
    category_df = filter_for_quote_tokens(category_df, allowed_quotes)
    category_pair_ids = category_df["pair_id"]

    print(f"Starting with {len(category_pair_ids)} tradeable pairs")
    our_pair_ids = list(category_pair_ids)

    # From these pair ids, see what trading pairs we have on Ethereum mainnet
    pairs_df = pairs_df[pairs_df["pair_id"].isin(our_pair_ids)]

    # Limit by supported DEX
    pairs_df = pairs_df[pairs_df["exchange_slug"].isin(exchanges)]
    print(f"After DEX filtering we have {len(pairs_df)} tradeable pairs")

    # Deduplicate trading pairs - Choose the best pair with the best volume
    deduplicated_df = deduplicate_pairs_by_volume(pairs_df)
    pairs_df = deduplicated_df
    print(f"Dropped duplicates length is {len(deduplicated_df)} pairs")

    # Load metadata
    pairs_df = load_token_metadata(pairs_df, client)

    assert "token_metadata" in pairs_df.columns
    assert "coingecko_categories" in pairs_df.columns

    # Scam filter using TokenSniffer
    pairs_df = pairs_df[pairs_df["tokensniffer_score"] >= 25]
    pairs_df = pairs_df.sort_values("volume", ascending=False)
    print(f"After TokenSniffer risk score filter we have {len(pairs_df)} pairs left")

    # Pull out categories for a singke token
    pairs_universe = PandasPairUniverse(pairs_df)
    joe_usdc = pairs_universe.get_pair_by_human_description(
        (ChainId.avalanche, "trader-joe", "JOE", "USDC.e"),
    )

    # Check metadata object has gone through all transformations
    assert joe_usdc.metadata
    assert joe_usdc.token_sniffer_data
    assert joe_usdc.coingecko_data
    categories = joe_usdc.metadata.get_coingecko_categories()



