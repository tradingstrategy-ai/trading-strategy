"""Test vault universe manamgenent."""
import datetime
from pathlib import Path

import pandas as pd
import pytest

from eth_defi.erc_4626.core import ERC4626Feature
from tradingstrategy.alternative_data.vault import load_vault_database, convert_vaults_to_trading_pairs, load_single_vault, DEFAULT_VAULT_BUNDLE, load_multiple_vaults, load_vault_price_data, convert_vault_prices_to_candles
from tradingstrategy.candle import GroupedCandleUniverse
from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import ExchangeType, ExchangeUniverse
from tradingstrategy.liquidity import GroupedLiquidityUniverse
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket


@pytest.fixture
def vault_database() -> Path:
    """Path to the test vault database."""
    return DEFAULT_VAULT_BUNDLE


def test_sideload_vaults(vault_database):
    """Load vaults from the database."""

    vault_universe = load_vault_database(vault_database)
    assert vault_universe.get_vault_count() == 7584

    ipor = vault_universe.get_by_chain_and_name(
        ChainId.base,
        "IPOR USDC Lending Optimizer Base",
    )
    assert ipor.denomination_token_symbol == "USDC"
    assert ipor.share_token_symbol == "ipUSDCfusion"
    assert ipor.deployed_at == datetime.datetime(2024, 11, 13, 9, 12, 11)
    assert ipor.management_fee == 0.01
    assert ipor.performance_fee == 0.10
    assert ipor.vault_address == "0x45aa96f0b3188d47a1dafdbefce1db6b37f58216"
    assert ipor.protocol_slug == "ipor"

    for v in vault_universe.vaults:
        assert "unknown" not in v.name


def test_vaults_as_trading_pairs(
    vault_database,
):
    """Export vaults as trading pairs."""

    exchange_universe = ExchangeUniverse({})

    vault_universe = load_vault_database(vault_database)
    vaults = vault_universe.export_all_vaults()
    exchange_data, pairs_df = convert_vaults_to_trading_pairs(vaults)

    assert len(exchange_data) > 1
    exchange_data.sort(key=lambda x: x.name)
    exchange = exchange_data[0]

    assert exchange.address == "0x0000000000000000000000000000000000000000"
    assert exchange.exchange_type == ExchangeType.erc_4626_vault
    assert exchange.name == "<unknown ERC-4626>"

    pairs_df = pairs_df.sort_values(by=["address"])
    row = pairs_df.iloc[0]
    assert row["token0_symbol"] == "wLOOKS"
    assert row["token1_symbol"] == "cLOOKS"
    assert row["fee"] == 0
    assert row["address"] == "0x000000000077ee1fcfe351df1ff22736e995806b"
    assert row["pair_slug"] == "compounding-looks"
    assert row["exchange_slug"] == "<unknown-erc-4626>"

    exchange_universe.add(exchange_data)
    pair_universe  = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)
    assert pair_universe.get_count() == 7584

    # Look up faux exchange data for vault protocol
    ipor_as_exchange = exchange_universe.get_by_chain_and_slug(ChainId.base, "ipor")
    assert ipor_as_exchange.address == "0x0000000000000000000000000000000000000000"

    # Look up vault using its address (symbolic lookup won't work)
    ipor = pair_universe.get_pair_by_smart_contract("0x45aa96f0b3188d47a1dafdbefce1db6b37f58216")

    assert ipor.address == "0x45aa96f0b3188d47a1dafdbefce1db6b37f58216"
    assert ipor.get_ticker() == "ipUSDCfusion-USDC"
    assert ipor.other_data["vault_features"] == [ERC4626Feature.ipor_like]


def test_load_single_vault():
    """Check load_single_vault()"""
    exchanges, df = load_single_vault(ChainId.base, "0x45aa96f0b3188d47a1dafdbefce1db6b37f58216")
    assert len(df) == 1
    assert len(exchanges) == 1


def test_load_multiple_vaults():
    """Check load_multiple_vaults()"""
    exchanges, df = load_multiple_vaults([(ChainId.base, "0x45aa96f0b3188d47a1dafdbefce1db6b37f58216")])
    assert len(df) == 1
    assert len(exchanges) == 1


def test_side_load_vault_price_data_hourly_resample():
    """Check load_vault_price_data() with 1h resampling"""

    vaults = [
        (ChainId.base, "0x45aa96f0b3188d47a1dafdbefce1db6b37f58216"),  # IPOR
        (ChainId.base, "0xad20523a7dc37babc1cc74897e4977232b3d02e5"),  # Gains
    ]
    exchanges, pairs_df = load_multiple_vaults(vaults)
    vault_prices_df = load_vault_price_data(pairs_df)
    assert len(vault_prices_df) == 403  # IPOR has 176 days worth of data

    # Create pair universe based on the vault data
    exchange_universe = ExchangeUniverse({e.exchange_id: e for e in exchanges})
    pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

    # Create price candles from vault share price scrape
    candle_df, liquidity_df = convert_vault_prices_to_candles(vault_prices_df, "1h")
    candle_universe = GroupedCandleUniverse(candle_df, time_bucket=TimeBucket.h1)
    assert candle_universe.get_candle_count() == 9626
    assert candle_universe.get_pair_count() == 2

    liquidity_universe = GroupedLiquidityUniverse(liquidity_df, time_bucket=TimeBucket.h1)
    assert liquidity_universe.get_sample_count() == 9626
    assert liquidity_universe.get_pair_count() == 2

    # Get share price as candles for a single vault
    ipor_usdc = pair_universe.get_pair_by_smart_contract("0x45aa96f0b3188d47a1dafdbefce1db6b37f58216")
    prices = candle_universe.get_candles_by_pair(ipor_usdc)
    assert len(prices) == 4201

    # Query single price sample
    timestamp = pd.Timestamp("2025-04-01 04:00")
    price, when = candle_universe.get_price_with_tolerance(
        pair=ipor_usdc,
        when=timestamp,
        tolerance=pd.Timedelta("2h"),
    )
    assert price == pytest.approx(1.0348826417292332)

    # Query TVL
    liquidity, when = liquidity_universe.get_liquidity_with_tolerance(
        pair_id=ipor_usdc.pair_id,
        when=timestamp,
        tolerance=pd.Timedelta("2h"),
    )
    assert liquidity == pytest.approx(1429198.98104)

    # Query TVL, another pair
    gains_usdc = pair_universe.get_pair_by_smart_contract("0xad20523a7dc37babc1cc74897e4977232b3d02e5")
    liquidity, when = liquidity_universe.get_liquidity_with_tolerance(
        pair_id=gains_usdc.pair_id,
        when=timestamp,
        tolerance=pd.Timedelta("2h"),
    )
    assert liquidity == pytest.approx(3194564.625348)


def test_side_load_vault_price_data_daily_resample():
    """Check load_vault_price_data() with 1d resampling"""

    vaults = [
        (ChainId.base, "0x45aa96f0b3188d47a1dafdbefce1db6b37f58216"),  # IPOR
        (ChainId.base, "0xad20523a7dc37babc1cc74897e4977232b3d02e5"),  # Gains
    ]
    exchanges, pairs_df = load_multiple_vaults(vaults)
    vault_prices_df = load_vault_price_data(pairs_df)
    assert len(vault_prices_df) == 403  # IPOR has 176 days worth of data

    # Create pair universe based on the vault data
    exchange_universe = ExchangeUniverse({e.exchange_id: e for e in exchanges})
    pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

    # Create price candles from vault share price scrape
    candle_df, liquidity_df = convert_vault_prices_to_candles(vault_prices_df, "1d")
    candle_universe = GroupedCandleUniverse(candle_df, time_bucket=TimeBucket.h1)
    assert candle_universe.get_candle_count() == 403
    assert candle_universe.get_pair_count() == 2

    # Query single price sample
    ipor_usdc = pair_universe.get_pair_by_smart_contract("0x45aa96f0b3188d47a1dafdbefce1db6b37f58216")
    timestamp = pd.Timestamp("2025-04-01 00:00")
    price, diff = candle_universe.get_price_with_tolerance(
        pair=ipor_usdc,
        when=timestamp,
        tolerance=pd.Timedelta("2h"),
    )
    assert price == pytest.approx(1.0350376272533817)
    assert diff == pd.Timedelta(0)

    # Check our data is daily
    prices = candle_universe.get_candles_by_pair(ipor_usdc)
    assert len(prices) == 176
    freq = pd.infer_freq(prices.index)
    assert freq == "D"

    # Check daily data is gapless
    expected_delta = pd.Timedelta(days=1)
    time_diff = prices.index.to_series().diff().dropna()
    assert all(time_diff == expected_delta)
