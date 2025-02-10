"""Token metadata loading tests."""
import datetime

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.utils.liquidity_filter import prefilter_pairs_with_tvl
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


def test_load_metadata_single_bad_token(
    persistent_test_client: Client,
):
    """"One of loaded tokens is unsupported"""
    client = persistent_test_client

    metadata = client.fetch_token_metadata(
        ChainId.ethereum,
        # AAVe, USDC
        addresses={"0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", "0xFFFF6991c6218b36c1d19d4a2e9eb0ce3606eb48"},
        cache=False,
    )
    assert len(metadata) == 1


def test_load_metadata_cache(
    persistent_test_client: Client,
):
    """"We correctly cache tokens"""
    client = persistent_test_client

    aave_address = "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9".lower()

    metadata = client.fetch_token_metadata(
        ChainId.ethereum,
        # AAVe, USDC
        addresses={aave_address},
        cache=False,
    )

    data = metadata[aave_address]
    assert data.disk_cached is False

    metadata = client.fetch_token_metadata(
        ChainId.ethereum,
        # AAVe, USDC
        addresses={aave_address},
        cache=True,
    )

    data = metadata[aave_address]
    assert data.disk_cached is True



def test_create_trading_universe_with_token_metadata(
    persistent_test_client: Client,
    default_pair_universe,
):
    """Create a trading universe using /token-metadata endpoint for filtering scams"""
    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()

    chain_id = ChainId.avalanche
    exchanges = {"trader-joe", "pangolin"}
    min_tvl = 100_000
    backtest_start = datetime.datetime(2024, 1, 1)
    backtest_end = datetime.datetime(2024, 1, 1)

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
        "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E".lower(),  # USDC
        "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7".lower(),  # WAVAX
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

    # Get TVL data for prefilteirng
    pairs_df = prefilter_pairs_with_tvl(
        client,
        pairs_df,
        chain_id=chain_id,
        min_tvl=min_tvl,
        start=backtest_start,
        end=backtest_end,
    )

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
    pairs_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)
    joe_usdc = pairs_universe.get_pair_by_human_description(
        (ChainId.avalanche, "trader-joe", "JOE", "WAVAX"),
    )

    # Check metadata object has gone through all transformations
    assert joe_usdc.metadata
    assert joe_usdc.token_sniffer_data
    assert joe_usdc.coingecko_data

    categories = joe_usdc.metadata.get_coingecko_categories()
    assert type(categories) == set
    assert "Decentralized Exchange (DEX)" in categories
    assert "Avalanche Ecosystem" in categories





