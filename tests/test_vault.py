"""Test vault universe manamgenent."""
import datetime
from pathlib import Path

import pytest

from tradingstrategy.alternative_data.vault import load_vault_database
from tradingstrategy.chain import ChainId


@pytest.fixture
def vault_database() -> Path:
    """Path to the test vault database."""
    return Path(__file__).parent / "vault-db.pickle"


def test_sideload_vaults(vault_database):
    """Load vaults from the database."""

    vault_universe = load_vault_database(vault_database)
    assert vault_universe.get_vault_count() == 7597

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
