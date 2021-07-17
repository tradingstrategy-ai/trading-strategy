import datetime

import pytest

from capitalgram.chain import ChainId
from capitalgram.pair import PairUniverse, PairType, DEXPair


def test_pair_pyarrow_schema():
    """We get a good Pyrarow schema for pair information serialisation and deserialisation."""

    schema = DEXPair.to_pyarrow_schema()
    assert str(schema[0].type) == "uint32"  # Primary key


def test_write_pyarrow_table():
    """We get a good Pyrarow schema for pair information serialisation and deserialisation."""

    items = [
        DEXPair(
            pair_id=1,
            chain_id=ChainId.ethereum,
            exchange_id=1,
            address="0x0000000000000000000000000000000000000000",
            dex_type=PairType.uniswap_v2,
            base_token_symbol="WETH",
            quote_token_symbol="USDC",
            token0_symbol="USDC",
            token1_symbol="WETH",
            token0_address="0x0000000000000000000000000000000000000000",
            token1_address="0x0000000000000000000000000000000000000000",
            first_swap_at_block_number=1,
            last_swap_at_block_number=1,
            first_swap_at=int(datetime.datetime(2020, 6, 4, 11, 42, 39).timestamp()),
            last_swap_at=int(datetime.datetime(2020, 6, 4, 11, 42, 39).timestamp()),
            flag_inactive=False,
            flag_blacklisted_manually=False,
            flag_unsupported_quote_token=False,
            flag_unknown_exchange=False
        )
    ]
    table = DEXPair.convert_to_pyarrow_table(items)
    assert len(table) == 1


