"""Dependency-free constants for the licence-gated Vaults Pro (Creem) datasets.

This module is deliberately import-light: it must be safe to import during client
setup, before we know whether the vault datasets are actually needed. The vault
dataset client :py:mod:`tradingstrategy.vault_data_client` pulls optional
dependencies (``zstandard`` via :py:mod:`tradingstrategy.alternative_data.vault`)
that are absent in minimal Pyodide builds, so reading the environment variable
name must not go through it.
"""

#: Environment variable holding the Creem licence key for vault datasets.
#:
#: This is the key emailed to the subscriber when they buy the Pro plan from
#: https://tradingstrategy.ai/vaults/datasets, formatted as five dash separated
#: groups. It is not :py:class:`tradingstrategy.client.Client`'s
#: ``TRADING_STRATEGY_API_KEY``, and it is not a ``creem_`` prefixed Creem
#: merchant API key, which belongs to the seller and is rejected here.
VAULT_PRO_API_KEY_ENV_VAR = "VAULT_PRO_API_KEY"
