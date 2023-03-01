import datetime

import pytest

from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import ExchangeType
from tradingstrategy.pair import DEXPair, PandasPairUniverse, resolve_pairs_based_on_ticker, NoPairFound


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

    schema = DEXPair.to_pyarrow_schema()
    assert str(schema[0].type) == "uint32"  # Primary key


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
    with pytest.raises(NoPairFound):
        pair_universe.get_pair_by_human_description(exchange_universe, desc)


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