"""Token metadata and TokenSniffer tests."""

import pytest

from eth_defi.token import USDC_NATIVE_TOKEN, WRAPPED_NATIVE_TOKEN
from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.utils.token_extra_data import load_token_metadata
from tradingstrategy.utils.token_filter import add_base_quote_address_columns, filter_for_stablecoins, StablecoinFilteringMode, filter_for_derivatives, filter_for_quote_tokens, deduplicate_pairs_by_volume, filter_by_token_sniffer_score


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


def test_create_trading_universe_tax_filter(
    persistent_test_client: Client,
    default_pair_universe,
):
    """Filter by tax in filter_by_token_sniffer_score(max_buy_tax)"""
    client = persistent_test_client

    # BETS
    # https://tradingstrategy.ai/trading-view/base/tokens/0x42069de48741db40aef864f8764432bbccbd0b69
    # 0x42069de48741db40aef864f8764432bbccbd0b69
    # 3% buy/sell tax

    chain_id = ChainId.base

    pairs_df = client.fetch_pair_universe().to_pandas()

    # Drop other chains to make the dataset smaller to work with
    chain_mask = pairs_df["chain_id"] == chain_id.value
    pairs_df = pairs_df[chain_mask]
    pairs_df = add_base_quote_address_columns(pairs_df)

    # Should give two pairs
    pairs_df = pairs_df.loc[pairs_df.base_token_address == "0x42069de48741db40aef864f8764432bbccbd0b69"]
    assert 0 < len(pairs_df) < 5

    # Load metadata
    pairs_df = load_token_metadata(pairs_df, client)

    assert "token_metadata" in pairs_df.columns
    assert "coingecko_categories" in pairs_df.columns

    # Scam filter using TokenSniffer
    none_filtered = filter_by_token_sniffer_score(pairs_df, risk_score=0, max_buy_tax=None)
    zero_filtered = filter_by_token_sniffer_score(pairs_df, risk_score=0, max_buy_tax=0.00)
    all_filtered = filter_by_token_sniffer_score(pairs_df, risk_score=0, max_buy_tax=0.01)
    high_filtered = filter_by_token_sniffer_score(pairs_df, risk_score=0, max_buy_tax=0.05)
    assert len(none_filtered) == 2
    assert len(zero_filtered) == 0
    assert len(all_filtered) == 0
    assert len(high_filtered) == 2
