"""Load token metadta."""
import datetime

import pandas as pd

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.utils.liquidity_filter import prefilter_pairs_with_tvl
from tradingstrategy.utils.token_filter import add_base_quote_address_columns, filter_for_stablecoins, StablecoinFilteringMode, filter_for_derivatives, filter_for_quote_tokens, deduplicate_pairs_by_volume


def test_load_token_metadata(
    persistent_test_client: Client,
):
    """Load CLMM data for two pairs on Uniswap v3."""

    client = persistent_test_client

    metadata = client.fetch_token_metadata(
        ChainId.ethereum,
        # AAVe, USDC
        {"0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"}
    )
    assert len(metadata) == 2

    # Loaded Aave
    assert "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9".lower() in metadata

    usdc = metadata["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"]
    print(usdc)


def test_load_metadata_bad_token(
    persistent_test_client: Client,
):
    client = persistent_test_client

    metadata = client.fetch_token_metadata(
        ChainId.ethereum,
        # AAVe, USDC
        {"0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", "0xFFFF6991c6218b36c1d19d4a2e9eb0ce3606eb48"}
    )
    assert len(metadata) == 1


def test_create_trading_universe_with_token_metadata(
    persistent_test_client: Client,
    default_pair_universe,
):
    """Create a trading universe"""
    client = persistent_test_client

    chain_id = ChainId.avalanche

    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()

    # Drop other chains to make the dataset smaller to work with
    chain_mask = pairs_df["chain_id"] == chain_id.value
    pairs_df = pairs_df[chain_mask]

    # Pull out our benchmark pairs ids.
    # We need to construct pair universe object for the symbolic lookup.
    pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

    category_df = pairs_df
    category_df = add_base_quote_address_columns(category_df)
    category_df = filter_for_stablecoins(category_df, StablecoinFilteringMode.only_volatile_pairs)
    category_df = filter_for_derivatives(category_df)

    allowed_quotes = {
        "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",  # USDC
        "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",  # WAVAX
    }

    category_df = filter_for_quote_tokens(category_df, allowed_quotes)
    category_pair_ids = category_df["pair_id"]

    print(f"Starting with {len(category_pair_ids)} tradeable pairs")
    our_pair_ids = list(category_pair_ids) + benchmark_pair_ids

    # From these pair ids, see what trading pairs we have on Ethereum mainnet
    pairs_df = pairs_df[pairs_df["pair_id"].isin(our_pair_ids)]

    # Limit by DEX
    pairs_df = pairs_df[pairs_df["exchange_slug"].isin(Parameters.exchanges)]
    print(f"After DEX filtering we have {len(pairs_df)} tradeable pairs")

    # Get TVL data for prefilteirng
    pairs_df = prefilter_pairs_with_tvl(
        client,
        pairs_df,
        chain_id=chain_id,
        min_tvl=500_000,
        start=datetime.datetime(2023, 1, 1),
        end=datetime.datetime(2023, 6 1),
    )


    # Deduplicate trading pairs - Choose the best pair with the best volume
    deduplicated_df = deduplicate_pairs_by_volume(pairs_df)
    pairs_df = deduplicated_df

    print(f"Dropped duplicates length is {len(deduplicated_df)} pairs")



    # Scam filter using TokenSniffer
    pairs_df = filter_scams(pairs_df, client, min_token_sniffer_score=Parameters.min_token_sniffer_score)
    pairs_df = pairs_df.sort_values("volume", ascending=False)
    print("After scam filter", len(pairs_df))

    uni_v2 = pairs_df.loc[pairs_df["exchange_slug"] == "uniswap-v2"]
    uni_v3 = pairs_df.loc[pairs_df["exchange_slug"] == "uniswap-v3"]
    print(f"Pairs on Uniswap v2: {len(uni_v2)}, Uniswap v3: {len(uni_v3)}")
    dataset = load_partial_data(
        client=client,
        time_bucket=Parameters.candle_time_bucket,
        pairs=pairs_df,
        execution_context=execution_context,
        universe_options=universe_options,
        liquidity=True,
        liquidity_time_bucket=TimeBucket.d1,
        liquidity_query_type=OHLCVCandleType.tvl_v2,
        lending_reserves=LENDING_RESERVES,
    )

    reserve_asset = USDC_NATIVE_TOKEN[chain_id.value]

    print("Creating trading universe")
    strategy_universe = TradingStrategyUniverse.create_from_dataset(
        dataset,
        reserve_asset=reserve_asset,
        forward_fill=True,  # We got very gappy data from low liquid DEX coins
    )

    assert isinstance(strategy_universe.data_universe.candles.df.index, pd.MultiIndex)

    # Tag benchmark/routing pairs tokens so they can be separated from the rest of the tokens
    # for the index construction.
    strategy_universe.warm_up_data()
    for pair_id in benchmark_pair_ids:
        pair = strategy_universe.get_pair_by_id(pair_id)
        pair.other_data["benchmark"] = False

    return strategy_universe

