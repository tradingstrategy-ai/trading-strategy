
"""Client lending dataset download and integrity tests"""

import logging
from pathlib import Path

import pytest

from tradingstrategy.chain import ChainId
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.client import Client
from tradingstrategy.lending import LendingCandleType, LendingReserveUniverse, LendingProtocolType, UnknownLendingReserve, LendingReserve

logger = logging.getLogger(__name__)


def test_client_download_lending_reserves_all_time(client: Client, cache_path: str):
    """Download all available data on lending protocol reserves."""
    df = client.fetch_lending_reserves_all_time()
    # Check we cached the file correctly
    assert Path(f"{cache_path}/lending-reserves-all.parquet").exists()
    assert len(df) > 100


def test_client_download_lending_reserve_universe(client: Client, cache_path: str):
    universe = client.fetch_lending_reserve_universe()
    # Check we cached the file correctly
    assert Path(f"{cache_path}/lending-reserve-universe.json").exists()
    assert len(universe.reserves) > 50


def test_fetch_lending_reserve_info(client: Client):
    universe = client.fetch_lending_reserve_universe()

    reserve = universe.get_reserve_by_id(2)
    assert reserve.reserve_id
    assert reserve.chain_id == 137
    assert reserve.asset_symbol == "LINK"
    assert reserve.asset_address == "0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39"
    assert reserve.asset_decimals == 18
    assert reserve.atoken_symbol == "aPolLINK"
    assert reserve.atoken_address == "0x191c10aa4af7c30e871e70c95db0e4eb77237530"
    assert reserve.atoken_decimals == 18

    reserve = universe.get_reserve_by_symbol_and_chain("USDC", ChainId.ethereum)
    assert reserve.reserve_id
    assert reserve.chain_id == 1
    assert reserve.asset_symbol == "USDC"
    assert reserve.asset_address == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    assert reserve.asset_decimals == 6
    assert reserve.atoken_symbol == "aEthUSDC"
    assert reserve.atoken_address == "0x98c23e9d8f34fefb1b7bd6a91b7ff122f4e16f5c"
    assert reserve.atoken_decimals == 6


def test_client_fetch_lending_candles(client: Client, cache_path: str):
    df = client.fetch_lending_candles_by_reserve_id(1, TimeBucket.h1)
    assert len(df) > 50
    assert len(list(Path(cache_path).glob("lending-candles-1h-between-any-and-any-*.parquet"))) == 1

    df = client.fetch_lending_candles_by_reserve_id(3, TimeBucket.d1, candle_type=LendingCandleType.supply_apr)
    assert len(df) > 50
    assert len(list(Path(cache_path).glob("lending-candles-1d-between-any-and-any-*.parquet"))) == 1


def test_resolve_lending_reserve(persistent_test_client):
    """Look up lending reserve by a human description"""
    client = persistent_test_client
    universe = client.fetch_lending_reserve_universe()

    usdt_reserve = universe.resolve_lending_reserve(
        (ChainId.polygon,
        LendingProtocolType.aave_v3,
        "USDT")
    )
    assert isinstance(usdt_reserve, LendingReserve)
    assert usdt_reserve.asset_address == '0xc2132d05d31c914a87c6611c10748aeb04b58e8f'
    assert usdt_reserve.asset_symbol == "USDT"
    assert usdt_reserve.asset_name == '(PoS) Tether USD'
    assert usdt_reserve.asset_decimals == 6
    assert usdt_reserve.atoken_symbol == "aPolUSDT"
    assert usdt_reserve.atoken_decimals == 6


def test_resolve_lending_reserve_unknown(persistent_test_client):
    """Look up lending reserve by a human description, but typo it out"""
    client = persistent_test_client
    universe = client.fetch_lending_reserve_universe()

    with pytest.raises(UnknownLendingReserve):
        universe.resolve_lending_reserve((ChainId.polygon, LendingProtocolType.aave_v3, "XXX"))

