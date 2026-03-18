"""Integration tests for vault metadata loading."""

import os

from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

from tradingstrategy.client import Client
from tradingstrategy.vault import Vault, VaultMetadata, VaultUniverse


def test_vault_universe_with_metadata(persistent_test_client: Client) -> None:
    """Test vault universe loading with full metadata.

    - Vault universe can be fetched from server
    - Vaults have metadata attached
    - VaultMetadata has key fields populated
    - Vault.get_metadata() returns the attached metadata
    - export_as_trading_pair() uses the attached metadata
    """
    vault_universe = persistent_test_client.fetch_vault_universe()
    assert isinstance(vault_universe, VaultUniverse)
    assert vault_universe.get_vault_count() > 0

    # Check first vault
    for vault in vault_universe.iterate_vaults():
        # Vault has metadata attached
        assert isinstance(vault, Vault)
        assert vault.metadata is not None
        assert isinstance(vault.metadata, VaultMetadata)

        # Key fields populated
        metadata = vault.metadata
        assert metadata.vault_name is not None
        assert metadata.protocol_slug is not None
        assert metadata.address is not None
        assert metadata.chain_id is not None
        assert metadata.period_results is not None or metadata.cagr is not None

        # get_metadata() returns same object
        assert vault.get_metadata() is vault.metadata

        # export_as_trading_pair uses attached metadata
        pair_data = vault.export_as_trading_pair()
        assert pair_data["token_metadata"] is vault.metadata

        break


def test_dex_pair_get_vault_metadata(persistent_test_client: Client) -> None:
    """Test DEXPair.get_vault_metadata() accessor."""
    from tradingstrategy.alternative_data.vault import convert_vaults_to_trading_pairs
    from tradingstrategy.exchange import ExchangeUniverse
    from tradingstrategy.pair import PandasPairUniverse

    vault_universe = persistent_test_client.fetch_vault_universe()
    exchanges, pairs_df = convert_vaults_to_trading_pairs(
        vault_universe.export_all_vaults()
    )

    exchange_universe = ExchangeUniverse({e.exchange_id: e for e in exchanges})
    pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

    # Get a vault pair
    for pair in pair_universe.iterate_pairs():
        metadata = pair.get_vault_metadata()
        if metadata is not None:
            assert isinstance(metadata, VaultMetadata)
            assert metadata.vault_name is not None
            break


def test_fetch_vault_price_history_normalises_timestamp_column(
    tmp_path: Path,
) -> None:
    """Test vault price history timestamp normalisation.

    1. Create a small parquet fixture where ``timestamp`` is stored in the index.
    2. Fetch the parquet through the new client method using a mocked transport.
    3. Confirm the returned frame exposes ``timestamp`` as a normal column.
    """
    parquet_path = tmp_path / "vault-price-history.parquet"
    indexed_df = pd.DataFrame(
        {
            "chain": [8453],
            "address": ["0x45aa96f0b3188d47a1dafdbefce1db6b37f58216"],
            "share_price": [1.01],
            "total_assets": [1000.0],
        },
        index=pd.DatetimeIndex([pd.Timestamp("2025-01-01 00:00:00")], name="timestamp"),
    )
    indexed_df.to_parquet(parquet_path)

    # 1. Create a small parquet fixture where ``timestamp`` is stored in the index.
    transport = Mock()
    transport.fetch_vault_price_history.return_value = parquet_path

    # 2. Fetch the parquet through the new client method using a mocked transport.
    client = Client(None, transport)
    result = client.fetch_vault_price_history()

    # 3. Confirm the returned frame exposes ``timestamp`` as a normal column.
    assert "timestamp" in result.columns
    assert result.iloc[0]["timestamp"] == pd.Timestamp("2025-01-01 00:00:00")


@pytest.mark.skipif(os.environ.get("TRADING_STRATEGY_API_KEY") is None, reason="Set TRADING_STRATEGY_API_KEY environment variable to run this test")
def test_fetch_vault_price_history(persistent_test_client: Client) -> None:
    """Test remote vault price history loading.

    1. Download cleaned vault price history through the persistent test client.
    2. Check the frame contains the expected core columns and timestamps.
    3. Confirm the parquet was cached on disk for later reuse.
    """
    # 1. Download cleaned vault price history through the persistent test client.
    history_df = persistent_test_client.fetch_vault_price_history()

    # 2. Check the frame contains the expected core columns and timestamps.
    assert len(history_df) > 0
    assert {"timestamp", "chain", "address", "share_price", "total_assets"}.issubset(history_df.columns)
    assert history_df["timestamp"].notna().all()

    # 3. Confirm the parquet was cached on disk for later reuse.
    cache_path = Path(persistent_test_client.transport.cache_path) / "vault-price-history.parquet"
    assert cache_path.exists()
