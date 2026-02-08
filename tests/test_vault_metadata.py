"""Integration tests for vault metadata loading."""

from tradingstrategy.vault import Vault, VaultMetadata, VaultUniverse


def test_vault_universe_with_metadata(persistent_test_client):
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


def test_dex_pair_get_vault_metadata(persistent_test_client):
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
