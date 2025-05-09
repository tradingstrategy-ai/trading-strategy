"""Vault data sideloading"""

import pickle
from pathlib import Path
from typing import Iterable

import pandas as pd
import zstandard

from eth_defi.erc_4626.core import ERC4262VaultDetection
from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import Exchange
from tradingstrategy.vault import VaultUniverse, Vault


#: Path to the bundled vault database
DEFAULT_VAULT_BUNDLE = Path(__file__).parent / ".." / "data_bundles" / "vault-db.pickle.zstd"


def load_vault_database(path: Path | None = None) -> VaultUniverse:
    """Load pickled vault database generated with an offline script.

    - For sideloading vault data

    - Normalises vault data in a good documented format

    - For the generation `see this tutorial <https://web3-ethereum-defi.readthedocs.io/tutorials/erc-4626-scan-prices.html>`__

    :param path:
        Path to the pickle file.

        If not given use the default location.

        Can be zstd compressed with .zstd suffix.
    """

    if path is None:
        path = DEFAULT_VAULT_BUNDLE

    assert path.exists(), f"No vault file: {path}"

    vault_db: dict

    if path.suffix == ".zstd":
        with zstandard.open(path, "rb") as inp:
            vault_db = pickle.load(inp)
    else:
        # Normal pickle
        vault_db = pickle.load(path.open("rb"))

    vaults = []

    #         data = {
    #             "Symbol": vault.symbol,
    #             "Name": vault.name,
    #             "Address": detection.address,
    #             "Denomination": vault.denomination_token.symbol if vault.denomination_token else None,
    #             "NAV": total_assets,
    #             "Protocol": get_vault_protocol_name(detection.features),
    #             "Mgmt fee": management_fee,
    #             "Perf fee": performance_fee,
    #             "Shares": total_supply,
    #             "First seen": detection.first_seen_at,
    #             "_detection_data": detection,
    #             "_denomination_token": denomination_token,
    #             "_share_token": vault.share_token.export() if vault.share_token else None,
    #         }

    for address, entry in vault_db.items():
        try:
            detection: ERC4262VaultDetection = entry["_detection_data"]

            if (not entry["Name"]) or (not entry["Denomination"]):
                # Skip invalid entries as all other requird data is missing
                continue

            if "unknown" in entry["Name"]:
                # Skip nameless / broken entries
                continue

            protocol_slug = entry["Protocol"].lower().replace(" ", "-")

            vault = Vault(
                chain_id=ChainId(detection.chain),
                name=entry["Name"],
                token_symbol=entry["Symbol"],
                vault_address=entry["Address"],
                denomination_token_address=entry["_denomination_token"]["address"],
                denomination_token_symbol=entry["_denomination_token"]["symbol"],
                denomination_token_decimals=entry["_denomination_token"]["decimals"],
                share_token_address=entry["_share_token"]["address"],
                share_token_symbol=entry["_share_token"]["symbol"],
                share_token_decimals=entry["_share_token"]["decimals"],
                protocol_name=entry["Protocol"],
                protocol_slug=protocol_slug,
                performance_fee=entry["Perf fee"],
                management_fee=entry["Mgmt fee"],
                deployed_at=detection.first_seen_at,
                features=detection.features,
                denormalised_data_updated_at=detection.updated_at,
                tvl=entry["NAV"],
                issued_shares=entry["Shares"],
            )
        except Exception as e:
            raise RuntimeError(f"Could not decode entry: {entry}") from e

        vaults.append(vault)

    return VaultUniverse(vaults)


def convert_vaults_to_trading_pairs(
    vaults: Iterable[Vault]
) -> tuple[list[Exchange], pd.DataFrame]:
    """Create a dataframe that contains vaults as trading pairs to be included alongside real trading pairs.

    - Generates :py:class:`tradingstrategy.pair.PandasPairUniverse` compatible dataframe for all vaults
    - Adds

    :return:
        Exchange data, pair dataframe tuple
    """

    exchanges = list(Exchange(**v.export_as_exchange()) for v in vaults)
    rows = [v.export_as_trading_pair() for v in vaults]
    pairs_df = pd.DataFrame(rows)

    return exchanges, pairs_df


def load_single_vault(
    chain_id: ChainId,
    vault_address: str,
    path=DEFAULT_VAULT_BUNDLE,
) -> tuple[list[Exchange], pd.DataFrame]:
    """Load a single bundled vault entry and return as pairs data.

    Example:

    .. code-block:: python

        vault_exchanges, vault_pairs_df = load_single_vault(ChainId.base, "0x45aa96f0b3188d47a1dafdbefce1db6b37f58216")
        exchange_universe.add(vault_exchanges)
        pairs_df = pd.concat([pairs_df, vault_pairs_df])

    """
    vault_universe = load_vault_database(path)
    vault_universe.limit_to_single(chain_id, vault_address)
    return convert_vaults_to_trading_pairs(vault_universe.export_all_vaults())



