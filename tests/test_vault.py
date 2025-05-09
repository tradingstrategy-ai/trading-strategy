"""Test vault universe manamgenent."""
import datetime
from pathlib import Path

import pytest

from eth_defi.erc_4626.core import ERC4626Feature
from tradingstrategy.alternative_data.vault import load_vault_database, convert_vaults_to_trading_pairs, load_single_vault
from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import ExchangeType, ExchangeUniverse
from tradingstrategy.pair import PandasPairUniverse


@pytest.fixture
def vault_database() -> Path:
    """Path to the test vault database."""
    return Path(__file__).parent / "vault-db.pickle"


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
    assert ipor.other_data["vault_features"] == {ERC4626Feature.ipor_like}


def test_load_single_vault():
    exchange_universe, df = load_single_vault(ChainId.base, "0x45aa96f0b3188d47a1dafdbefce1db6b37f58216")
    assert len(df) == 1