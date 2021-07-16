import datetime

import pytest

from capitalgram.chain import ChainId
from capitalgram.pair import PairUniverse, PairType, DEXPair

# Taken from the server HTTP reply
SIMPLE_THREE_WEEKS_WETH_USDT_UNIVERSE = """ {"last_updated_at": 1625673351.0, "pairs": [{"chain_id": 1, "address": "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", "dex_type": "uni_v2", "base_token_symbol": "WETH", "quote_token_symbol": "USDC", "token0_symbol": "USDC", "token1_symbol": "WETH", "token0_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "token1_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "first_swap_at_block_number": 10008566, "last_swap_at_block_number": 10199108, "first_swap_at": 1588705772.0, "last_swap_at": 1591263767.0, "flag_inactive": false, "flag_blacklisted_manually": false, "flag_unsupported_quote_token": false, "flag_unknown_exchange": true, "exchange_name": null, "exchange_address": null, "flag_not_enough_swaps": null, "flag_on_trustwallet": null, "flag_on_etherscan": null, "flag_code_verified": null, "fee": null, "trustwallet_info_checked_at": null, "etherscan_info_checked_at": null, "etherscan_code_verified_checked_at": null, "blacklist_reason": null, "trustwallet_info": null, "etherscan_info": null, "buy_count_all_time": 186, "sell_count_all_time": 180, "buy_volume_all_time": 32833.47290600001, "sell_volume_all_time": 32442.996082, "buy_volume_30d": 32833.47290600001, "sell_volume_30d": 32443.007082000004, "same_pair_on_other_exchanges": null, "bridged_pair_on_other_exchanges": null, "fake_pairs": null}]}"""


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


@pytest.mark.skip(msg="Needs a new sample data dump")
def test_decode_encode_universe():
    universe: PairUniverse = PairUniverse.from_json(SIMPLE_THREE_WEEKS_WETH_USDT_UNIVERSE)
    assert universe.pairs[0].chain_id == ChainId.ethereum
    assert universe.pairs[0].dex_type == PairType.uniswap_v2
    encoded = universe.to_json()
    decoded = PairUniverse.from_json(encoded)
    assert decoded == universe

