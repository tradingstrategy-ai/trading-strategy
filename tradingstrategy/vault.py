""""Vault data for EIP-4626 and other digital asset management protocols."""
import datetime
from dataclasses import dataclass, field

from eth_defi.erc_4626.core import ERC4626Feature

from tradingstrategy.chain import ChainId
from tradingstrategy.types import Percent, NonChecksummedAddress


@dataclass(slots=True, frozen=True)
class Vault:
    """Vault core data..

    - Normalised core dat across vaults

    - See :py:func:`tradingstrategy.alternative_data.vault.load_vault_database` for the data source.
    """

    #: The chain this vault is on
    chain_id: ChainId

    #: Main vault address
    vault_address: NonChecksummedAddress

    #: Deposit token (USDC, etc.)
    #:
    denomination_token_address: NonChecksummedAddress

    #: "USDC"
    denomination_token_symbol: str

    #: Share token address
    #:
    #: Differs for multitoken vaults from vaults address.
    share_token_address: NonChecksummedAddress

    #: "MyVault1"
    share_token_symbol: str

    #: Protocol slug for this vault
    protocol_slug: str

    #: Vault name
    name: str

    #: Vault share token symbol
    token: str

    #: Feature flags
    features: set[ERC4626Feature] = field(default_factory=set)

    #: Approx timestamp when this vault was deployed
    deployed_at: datetime.datetime | None = None

    #: When fee, TVL and such data was updated.
    #:
    denormalised_data_updated_at: datetime.datetime | None = None

    #: Latest management fee.
    #:
    #: This change over time
    management_fee: Percent | None = None

    #: Latest performance fee.
    #:
    #: This change over time
    performance_fee: Percent | None = None

    #: TVL in denomination token.
    tvl: float | None = None

    #: Total number of shares minted
    issued_shares: float | None = None

    def is_4626(self) -> bool:
        return ERC4626Feature.erc_4626 in self.features


class VaultUniverse:
    """Vault universe of all accessible vaults."""

    def __init__(self, vaults: list[Vault]):
        assert len(vaults) > 0
        assert isinstance(vaults[0], Vault)
        self.vaults = vaults

    def get_by_chain_and_name(self, chain_id: ChainId, name: str) -> Vault | None:
        """Get vault by chain id and name."""
        assert isinstance(chain_id, ChainId)
        assert type(name) == str
        for vault in self.vaults:
            if vault.chain_id == chain_id and vault.name == name:
                return vault
        return None

    def get_vault_count(self) -> int:
        """Get number of vaults in the universe."""
        return len(self.vaults)
