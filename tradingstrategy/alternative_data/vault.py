"""Vault data sideloading"""
import pickle
from pathlib import Path

from eth_defi.erc_4626.core import ERC4262VaultDetection
from tradingstrategy.chain import ChainId
from tradingstrategy.vault import VaultUniverse, Vault


def load_vault_database(path: Path | None = None) -> VaultUniverse:
    """Load pickled vault database generated with an offline script.

    - For sideloading vault data

    - Normalises vault data in a good documented format

    - For the generation `see this tutorial <https://web3-ethereum-defi.readthedocs.io/tutorials/erc-4626-scan-prices.html>`__

    :param path:
        Path to the pickle file.

        If not given use the default location.
    """

    if not path:
        path = Path("~/.tradingstrategy/vaults/vault-db.pickle").expanduser()

    assert path.exists(), f"No vault file: {path}"

    vault_db: dict
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

            if entry["Name"] == "" or entry["Denomination"] is None or entry["Denomination"] == "":
                # Skip invalid entries as all other requird data is missing
                continue

            vault = Vault(
                chain_id=ChainId(detection.chain),
                name=entry["Name"],
                token=entry["Symbol"],
                vault_address=entry["Address"],
                denomination_token_address=entry["_denomination_token"]["address"],
                denomination_token_symbol=entry["_denomination_token"]["symbol"],
                share_token_address=entry["_share_token"]["address"],
                share_token_symbol=entry["_share_token"]["symbol"],
                protocol_slug=entry["Protocol"],
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
