"""Test vault universe manamgenent."""
from pathlib import Path

import pytest

from tradingstrategy.alternative_data.vault import load_vault_database


@pytest.fixture
def vault_database() -> Path:
    """Path to the test vault database."""
    return Path(__file__).parent / "vault-db.pickle"


def test_sideload_vaults(vault_database):
    """Load vaults from the database."""

    vault_universe = load_vault_database(vault_database)
    assert vault_universe.get_vault_count() == 1000

