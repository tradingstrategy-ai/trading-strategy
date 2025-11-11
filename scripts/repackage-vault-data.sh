#!/bin/bash
#
# Copy eth_defi scanned data and make it part of our Python package
#
set -u
set -e

# Our tests use this fixed data set.
# Please do not update.
# cp ~/.tradingstrategy/vaults/vault-prices.parquet tradingstrategy/data_bundles/

zstd -22 --ultra -f -o tradingstrategy/data_bundles/vault-db.pickle.zstd ~/.tradingstrategy/vaults/vault-db.pickle

# Check the generated file loads good and has expected vault count

python -c 'from tradingstrategy.alternative_data.vault import DEFAULT_VAULT_BUNDLE ; print(f"Default vault bundle is {DEFAULT_VAULT_BUNDLE.resolve()}")'
python -c 'from tradingstrategy.alternative_data.vault import load_vault_database ; db = load_vault_database(filter_bad_entries=False) ; print(f"We have metadata for {db.get_vault_count()} vaults")'