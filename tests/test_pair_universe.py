import datetime
from typing import Set, List, Tuple

import pytest

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.exchange import ExchangeType, ExchangeNotFoundError
from tradingstrategy.pair import PandasPairUniverse, resolve_pairs_based_on_ticker, PairNotFoundError, DEXPair, generate_address_columns, TokenNotFound, MultipleTokensWithSymbol


@pytest.fixture
def sample_pair() -> DEXPair:
    return DEXPair(
            pair_id=1,
            chain_id=ChainId.ethereum,
            exchange_id=1,
            exchange_slug="uniswap-v2",
            pair_slug="eth-usdc",
            address="0x0000000000000000000000000000000000000001",
            dex_type=ExchangeType.uniswap_v2,
            base_token_symbol="WETH",
            quote_token_symbol="USDC",
            token0_decimals=6,
            token1_decimals=18,
            token0_symbol="USDC",
            token1_symbol="WETH",
            token0_address="0x0000000000000000000000000000000000000002",
            token1_address="0x0000000000000000000000000000000000000003",
            first_swap_at_block_number=1,
            last_swap_at_block_number=1,
            first_swap_at=int(datetime.datetime(2020, 6, 4, 11, 42, 39).timestamp()),
            last_swap_at=int(datetime.datetime(2020, 6, 4, 11, 42, 39).timestamp()),
            flag_inactive=False,
            flag_blacklisted_manually=False,
            flag_unsupported_quote_token=False,
            flag_unknown_exchange=False
        )


def test_pair_pyarrow_schema():
    """We get a good Pyrarow schema for pair information serialisation and deserialisation."""

    DEXPair.to_pyarrow_schema()


def test_write_pyarrow_table(sample_pair):
    """We get a good Pyrarow schema for pair information serialisation and deserialisation."""

    items = [
        sample_pair
    ]
    table = DEXPair.convert_to_pyarrow_table(items)
    assert len(table) == 1


def test_pair_info_url(sample_pair):
    """We get a good info URLs"""

    p = sample_pair
    assert p.get_trading_pair_page_url() == "https://tradingstrategy.ai/trading-view/ethereum/uniswap-v2/eth-usdc"
    assert p.base_token_decimals == 18
    assert p.quote_token_decimals == 6


def test_get_all_tokens(sample_pair):
    """Get all tokens in trading pairs."""

    items = [
        sample_pair
    ]
    df = DEXPair.convert_to_dataframe(items)
    universe = PandasPairUniverse(df)
    tokens = universe.get_all_tokens()
    assert len(tokens) == 2


def test_resolve_pairs_based_on_ticker(persistent_test_client):
    """Check that we can find multiple pairs."""

    client = persistent_test_client
    pairs_df = client.fetch_pair_universe().to_pandas()

    tickers = {
        ("WBNB", "BUSD"),
        ("Cake", "WBNB"),
    }

    # ticker -> pd.Series row map for pairs
    filtered_pairs_df = resolve_pairs_based_on_ticker(
        pairs_df,
        ChainId.bsc,
        "pancakeswap-v2",
        tickers
    )

    assert len(filtered_pairs_df) == 2
    wbnb_busd = filtered_pairs_df.loc[
        (filtered_pairs_df["base_token_symbol"] == "WBNB") &
        (filtered_pairs_df["quote_token_symbol"] == "BUSD")
    ].squeeze()
    assert wbnb_busd["buy_volume_30d"] > 0


def test_resolve_pairs_based_on_ticker_with_fee(persistent_test_client):
    """Check that we can find multiple pairs with specified fee."""

    client = persistent_test_client
    pairs_df = client.fetch_pair_universe().to_pandas()

    tickers = {
        ("WETH", "USDC", 5),
        ("DAI", "USDC"),
    }

    filtered_pairs_df = resolve_pairs_based_on_ticker(
        pairs_df,
        ChainId.ethereum,
        "uniswap-v3",
        tickers
    )

    assert len(filtered_pairs_df) == 2


def test_resolve_pairs_based_on_ticker_with_description_and_fee(persistent_test_client):
    """Check that we can find multiple pairs with specified fee using new API."""

    client = persistent_test_client
    pairs_df = client.fetch_pair_universe().to_pandas()

    pairs = {
        (ChainId.ethereum, "uniswap-v3", "WETH", "USDC", 0.0005),
        (ChainId.ethereum, "uniswap-v3", "DAI", "USDC"),
    }

    filtered_pairs_df = resolve_pairs_based_on_ticker(
        pairs_df,
        pairs=pairs,
    )

    assert len(filtered_pairs_df) == 2

def test_get_token(persistent_test_client):
    """Check that we can decode token information fom pair data."""

    client = persistent_test_client
    pairs_df = client.fetch_pair_universe().to_pandas()
    pair_universe = PandasPairUniverse(pairs_df)

    # Do tests with BNB Chain tokens

    token = pair_universe.get_token("0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c")
    assert token.symbol == "WBNB"
    assert token.decimals == 18
    assert token.chain_id == ChainId.bsc

    token = pair_universe.get_token("0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c".upper())
    assert token.symbol == "WBNB"

    token = pair_universe.get_token("0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d")
    assert token.symbol == "USDC"
    assert token.decimals == 18
    assert token.chain_id == ChainId.bsc


def test_get_token_by_symbol(persistent_test_client):
    """Look up tokens by symbol"""

    client = persistent_test_client
    pairs_df = client.fetch_pair_universe().to_pandas()
    pair_universe = PandasPairUniverse(pairs_df)

    # Auto pick by vol
    token = pair_universe.get_token_by_symbol("WMATIC", chain_id=ChainId.polygon)
    assert token.symbol == "WMATIC"
    assert token.decimals == 18
    assert token.chain_id == ChainId.polygon

    # Single match only
    token = pair_universe.get_token_by_symbol("CAGA", chain_id=ChainId.ethereum, pick_by_highest_volume=False)
    assert token.symbol == "CAGA"
    assert token.decimals == 18

    # Not found
    with pytest.raises(TokenNotFound):
        pair_universe.get_token_by_symbol("NONEXISTINGCOIN")

    # Multiple matches, no auto pick
    with pytest.raises(MultipleTokensWithSymbol):
        pair_universe.get_token_by_symbol("WMATIC", pick_by_highest_volume=False)


def test_resolve_based_on_human_description(persistent_test_client):
    """Human description resolves pairs.."""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()
    pair_universe = PandasPairUniverse(pairs_df)

    desc = (ChainId.bsc, "pancakeswap-v2", "WBNB", "BUSD")
    bnb_busd = pair_universe.get_pair_by_human_description(exchange_universe, desc)
    assert bnb_busd.base_token_symbol == "WBNB"
    assert bnb_busd.quote_token_symbol == "BUSD"
    assert bnb_busd.buy_volume_30d > 1_000_000

    desc = (ChainId.bsc, "pancakeswap-v2", "MIKKO", "BUSD")
    with pytest.raises(PairNotFoundError) as excinfo:
        pair_universe.get_pair_by_human_description(exchange_universe, desc)
    
    assert "MIKKO" in excinfo.value.args[0]


def test_get_pair(persistent_test_client):
    """Human description get_pair() resolves pairs."""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()
    pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

    bnb_busd = pair_universe.get_pair(
        ChainId.bsc,
        "pancakeswap-v2",
        "WBNB",
        "BUSD"
    )
    assert bnb_busd.base_token_symbol == "WBNB"
    assert bnb_busd.quote_token_symbol == "BUSD"
    assert bnb_busd.buy_volume_30d > 1_000_000


def test_fee_tier_uniswap_v2(persistent_test_client):
    """Resolve Uniswap v2 pair using fee tier.
    """

    pair_human_description = (ChainId.ethereum, "uniswap-v2", "EUL", "WETH", 0.0030)  # Euler 30 bps fee

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()
    pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

    exchange = exchange_universe.get_by_chain_and_slug(pair_human_description[0], pair_human_description[1])
    pair = pair_universe.get_one_pair_from_pandas_universe(
        exchange.exchange_id,
        pair_human_description[2],
        pair_human_description[3],
        fee_tier=pair_human_description[4],
        pick_by_highest_vol=True,
    )
    assert pair.exchange_slug == "uniswap-v2"
    assert pair.base_token_symbol == "EUL"
    assert pair.quote_token_symbol == "WETH"
    assert pair.fee == 30

    with pytest.raises(ExchangeNotFoundError) as excinfo:
        exchange = exchange_universe.get_by_chain_and_slug(ChainId.ethereum, "alex-v2")

    assert excinfo.value.args[0] == 'The trading universe does not contain exchange data on chain ethereum for exchange_slug alex-v2. This might be a problem in your data loading and filtering. \n                \n    Use tradingstrategy.ai website to explore DEXs.\n    \n    Here is a list of DEXes: https://tradingstrategy.ai/trading-view/exchanges\n    \n    For any further questions join our Discord: https://tradingstrategy.ai/community'


def test_fee_tier_uniswap_v3(persistent_test_client):
    """Resolve Uniswap v3 pair using fee tier.
    """

    pair_human_description = (ChainId.ethereum, "uniswap-v3", "EUL", "WETH", 0.01)  # Euler 100 bps fee

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()
    pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

    exchange = exchange_universe.get_by_chain_and_slug(pair_human_description[0], pair_human_description[1])
    pair = pair_universe.get_one_pair_from_pandas_universe(
        exchange.exchange_id,
        pair_human_description[2],
        pair_human_description[3],
        fee_tier=pair_human_description[4],
        pick_by_highest_vol=True,
    )
    assert pair.exchange_slug == "uniswap-v3"
    assert pair.base_token_symbol == "EUL"
    assert pair.quote_token_symbol == "WETH"
    assert pair.fee == 100


def test_lower_fee_tier_uniswap_v3(persistent_test_client):
    """Resolve Uniswap v3 pair using fee tier, automatically pick the lowest fee tier.
    """

    pair_human_description = (ChainId.ethereum, "uniswap-v3", "WETH", "USDC")  # ETH 1 bps fee

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()
    pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

    exchange = exchange_universe.get_by_chain_and_slug(pair_human_description[0], pair_human_description[1])
    pair = pair_universe.get_one_pair_from_pandas_universe(
        exchange.exchange_id,
        pair_human_description[2],
        pair_human_description[3],
        fee_tier=None,
        pick_by_highest_vol=True,
    )
    assert pair.exchange_slug == "uniswap-v3"
    assert pair.base_token_symbol == "WETH"
    assert pair.quote_token_symbol == "USDC"
    assert pair.fee == 1


def test_multiple_get_pair_by_human_description(persistent_test_client):
    """Resolve all kind of pairs.

    - Test multiple chains

    - Test multiple exchanges

    - Test multiple fee models
    """

    pair_human_descriptions = (
        (ChainId.ethereum, "uniswap-v2", "WETH", "USDC"),  # ETH
        (ChainId.ethereum, "uniswap-v2", "EUL", "WETH", 0.0030),  # Euler 30 bps fee
        (ChainId.ethereum, "uniswap-v3", "EUL", "WETH", 0.0100),  # Euler 100 bps fee
        (ChainId.ethereum, "uniswap-v2", "MKR", "WETH"),  # MakerDAO
        (ChainId.ethereum, "uniswap-v2", "HEX", "WETH"),  # MakerDAO
        (ChainId.ethereum, "uniswap-v2", "FNK", "USDT"),  # Finiko
        (ChainId.ethereum, "sushi", "AAVE", "WETH"),  # AAVE
        (ChainId.ethereum, "sushi", "COMP", "WETH"),  # Compound
        (ChainId.ethereum, "sushi", "WETH", "WBTC"),  # BTC
        (ChainId.ethereum, "sushi", "ILV", "WETH"),  # Illivium
        (ChainId.ethereum, "sushi", "DELTA", "WETH"),  # Delta
        (ChainId.ethereum, "sushi", "UWU", "WETH"),  # UwU lend
        (ChainId.ethereum, "uniswap-v2", "UNI", "WETH"),  # UNI
        (ChainId.ethereum, "uniswap-v2", "CRV", "WETH"),  # Curve
        (ChainId.ethereum, "sushi", "SUSHI", "WETH"),  # Sushi
        (ChainId.bsc, "pancakeswap-v2", "WBNB", "BUSD"),  # BNB
        (ChainId.bsc, "pancakeswap-v2", "Cake", "BUSD"),  # Cake
        (ChainId.bsc, "pancakeswap-v2", "MBOX", "BUSD"),  # Mobox
        (ChainId.bsc, "pancakeswap-v2", "RDNT", "WBNB"),  # Radiant
        (ChainId.polygon, "quickswap", "WMATIC", "USDC"),  # Matic
        (ChainId.polygon, "quickswap", "QI", "WMATIC"),  # QiDao
        (ChainId.polygon, "sushi", "STG", "USDC"),  # Stargate
        (ChainId.avalanche, "trader-joe", "WAVAX", "USDC"),  # Avax
        (ChainId.avalanche, "trader-joe", "JOE", "WAVAX"),  # TraderJoe
        (ChainId.avalanche, "trader-joe", "GMX", "WAVAX"),  # GMX
        (ChainId.arbitrum, "camelot", "ARB", "WETH"),  # ARB
        (ChainId.arbitrum, "uniswap-v3", "ARB", "USDC", 0.0005),  # Arbitrum, 5 BPS fee
        # (ChainId.arbitrum, "sushi", "MAGIC", "WETH"),  # Magic
    )

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()
    pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

    pairs: List[DEXPair]
    pairs = [pair_universe.get_pair_by_human_description(exchange_universe, d) for d in pair_human_descriptions]

    assert len(pairs) == 27
    assert pairs[0].exchange_slug == "uniswap-v2"
    assert pairs[0].get_ticker() == "WETH-USDC"

    assert pairs[1].exchange_slug == "uniswap-v2"
    assert pairs[1].get_ticker() == "EUL-WETH"

    assert pairs[2].exchange_slug == "uniswap-v3"
    assert pairs[2].get_ticker() == "EUL-WETH"

    assert pairs[-1].exchange_slug == "uniswap-v3"
    assert pairs[-1].get_ticker() == "ARB-USDC"


def test_generate_address_columns(persistent_test_client):
    """Add base_token_address and quote_token_address columns
    """

    pair_human_description = (ChainId.ethereum, "uniswap-v3", "WETH", "USDC")  # ETH 1 bps fee

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()

    old_len = len(pairs_df)
    pairs_df = generate_address_columns(pairs_df)
    assert len(pairs_df) == old_len

    assert "base_token_address" in pairs_df.columns
    assert "quote_token_address" in pairs_df.columns

    pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

    pair = pair_universe.get_pair_by_human_description(
        exchange_universe,
        pair_human_description
    )

    assert pair.base_token_address == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"  # WETH
    assert pair.quote_token_address == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # USDC


def test_create_pair_universe_with_fee(persistent_test_client: Client):
    """Test generic create_pair_universe()"""
    client = persistent_test_client
    pairs_df = client.fetch_pair_universe().to_pandas()

    # Create a trading pair universe for a single trading pair
    #
    # WMATIC-USD on Uniswap v3 on Polygon, 5 BPS fee tier
    #
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(ChainId.polygon, "uniswap-v3", "WMATIC", "USDC", 0.0005)],
        )
    assert pair_universe.get_count() == 1
    pair = pair_universe.get_single()
    assert pair.base_token_symbol == "WMATIC"
    assert pair.quote_token_symbol == "USDC"
    assert pair.fee_tier == 0.0005  # BPS


def test_create_two_pair_universe_with_fee(persistent_test_client: Client):
    """Test generic create_pair_universe()"""
    client = persistent_test_client
    pairs_df = client.fetch_pair_universe().to_pandas()

    # Create a trading pair universe for a single trading pair
    #
    # WMATIC-USD on Uniswap v3 on Polygon, 5 BPS fee tier and 30 BPS fee tier
    #
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [
                (ChainId.polygon, "uniswap-v3", "WMATIC", "USDC", 0.0005),
                (ChainId.polygon, "uniswap-v3", "WMATIC", "USDC", 0.0030)
            ],
        )
    assert pair_universe.get_count() == 2


