#!/bin/bash
#
# Copy eth_defi scanned data and make it part of our Python package
#
set -u
set -e

cp ~/.tradingstrategy/vaults/vault-prices.parquet tradingstrategy/alternative_data/
zstd -f -o tradingstrategy/alternative_data/vault-db.pickle.zstd ~/.tradingstrategy/vaults/vault-db.pickle

