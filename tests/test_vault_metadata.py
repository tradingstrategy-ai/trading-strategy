"""Integration tests for vault metadata loading."""

import os

from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.vault import Vault, VaultMetadata, VaultUniverse


def _make_vault(chain_id, denomination_token_address, denomination_token_symbol="USDC", name="TestVault", vault_address=None):
    """Build a minimal Vault for unit testing."""
    addr = vault_address or f"0x{chain_id:040x}"
    return Vault(
        chain_id=ChainId(chain_id),
        vault_address=addr,
        denomination_token_address=denomination_token_address,
        denomination_token_symbol=denomination_token_symbol,
        denomination_token_decimals=6,
        share_token_address=f"0xshare{addr[6:]}",
        share_token_symbol="sVault",
        share_token_decimals=6,
        protocol_name="test",
        protocol_slug="test",
        name=name,
        token_symbol="sVault",
    )


def test_vault_universe_with_metadata(
    persistent_test_client: Client,
    tmp_path: Path,
) -> None:
    """Test vault universe loading with full metadata.

    1. Download vault metadata into a pytest-provided temporary location.
    2. Confirm vault metadata is attached correctly to the returned universe.
    3. Confirm the downloaded JSON was stored under the requested root.
    """
    download_root = tmp_path / "vault-downloads"

    # 1. Download vault metadata into a pytest-provided temporary location.
    vault_universe = persistent_test_client.fetch_vault_universe(download_root=download_root)
    assert isinstance(vault_universe, VaultUniverse)
    assert vault_universe.get_vault_count() > 0

    # 2. Confirm vault metadata is attached correctly to the returned universe.
    for vault in vault_universe.iterate_vaults():
        assert isinstance(vault, Vault)
        assert vault.metadata is not None
        assert isinstance(vault.metadata, VaultMetadata)

        metadata = vault.metadata
        assert metadata.vault_name is not None
        assert metadata.protocol_slug is not None
        assert metadata.address is not None
        assert metadata.chain_id is not None
        assert metadata.period_results is not None or metadata.cagr is not None

        assert vault.get_metadata() is vault.metadata

        pair_data = vault.export_as_trading_pair()
        assert pair_data["token_metadata"] is vault.metadata

        break

    # 3. Confirm the downloaded JSON was stored under the requested root.
    assert (download_root / "vault-universe.json").exists()


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
def test_fetch_vault_price_history(
    persistent_test_client: Client,
    tmp_path: Path,
) -> None:
    """Test remote vault price history loading.

    1. Download cleaned vault price history through the persistent test client into a temporary root.
    2. Check the frame contains the expected core columns and timestamps.
    3. Confirm the parquet was cached on disk for later reuse.
    """
    download_root = tmp_path / "vault-downloads"

    # 1. Download cleaned vault price history through the persistent test client into a temporary root.
    history_df = persistent_test_client.fetch_vault_price_history(download_root=download_root)

    # 2. Check the frame contains the expected core columns and timestamps.
    assert len(history_df) > 0
    assert {"timestamp", "chain", "address", "share_price", "total_assets"}.issubset(history_df.columns)
    assert history_df["timestamp"].notna().all()

    # 3. Confirm the parquet was cached on disk for later reuse.
    assert (download_root / "vault-price-history.parquet").exists()


def test_limit_to_native_usdc():
    """Verify address-aware native USDC filtering on VaultUniverse.

    Vaults with symbol "USDC" but a non-native or missing denomination
    address must be excluded when CCTP bridge generation is active.
    """
    from eth_defi.token import USDC_NATIVE_TOKEN

    native_addr = USDC_NATIVE_TOKEN[1]  # Ethereum native USDC
    vault_a = _make_vault(1, native_addr, name="NativeUSDC", vault_address="0xaaa0000000000000000000000000000000000001")
    vault_b = _make_vault(1, "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef", name="BridgedUSDC", vault_address="0xbbb0000000000000000000000000000000000002")
    vault_c = _make_vault(1, None, name="MissingAddr", vault_address="0xccc0000000000000000000000000000000000003")

    # All three together — only vault_a should survive
    universe = VaultUniverse([vault_a, vault_b, vault_c])
    filtered = universe.limit_to_native_usdc()
    assert filtered.get_vault_count() == 1
    remaining = list(filtered.iterate_vaults())
    assert remaining[0].name == "NativeUSDC"

    # Only non-native vaults — should raise
    universe_bad = VaultUniverse([vault_b, vault_c])
    with pytest.raises(AssertionError, match="No vaults with native USDC"):
        universe_bad.limit_to_native_usdc()
