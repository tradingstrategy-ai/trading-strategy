import datetime

import pytest

from tradingstrategy.chain import ChainId
from tradingstrategy.pair import LegacyPairUniverse, PairType, DEXPair, PandasPairUniverse


@pytest.fixture
def sample_pair() -> DEXPair:
    return DEXPair(
            pair_id=1,
            chain_id=ChainId.ethereum,
            exchange_id=1,
            exchange_slug="uniswap-v2",
            pair_slug="eth-usdc",
            address="0x0000000000000000000000000000000000000001",
            dex_type=PairType.uniswap_v2,
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
    """Get all tokens in tradin pairs."""

    items = [
        sample_pair
    ]
    df = DEXPair.convert_to_dataframe(items)
    universe = PandasPairUniverse(df)
    tokens = universe.get_all_tokens()
    assert len(tokens) == 2


