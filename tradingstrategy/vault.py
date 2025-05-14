""""Vault data for EIP-4626 and other digital asset management protocols."""
import datetime
from dataclasses import dataclass, field
from typing import Iterable, TypeAlias, Any

try:
    from eth_defi.erc_4626.core import ERC4626Feature
except ImportError:
    # Spoof for soft imports
    ERC4626Feature: TypeAlias = str

from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import ExchangeType
from tradingstrategy.types import Percent, NonChecksummedAddress
from tradingstrategy.types import SPECIAL_PAIR_ID_RANGE


@dataclass(slots=True, frozen=True)
class VaultMetadata:
    """User in pair dataframe for vault specific data.
    """

    #: Like "Harvest Autopilot"
    vault_name: str

    #: Like "Ipor"
    protocol_name: str

    #: Vault protocol slug e.g. "ipor"
    protocol_slug: str

    #: Supported features by this vault
    #:
    #: Must be JSON serialisable, as this will be passed around to JSON state
    features: list[ERC4626Feature]

    #: Performance fee.
    #:
    #: .. note::
    #:
    #:   As this changes over time, never use this fixed value in backtesting or live trading.
    #:
    performance_fee: Percent | None = None

    #: Management fee
    #:
    #: .. note::
    #:
    #:   As this changes over time, never use this fixed value in backtesting or live trading.
    #:
    management_fee: Percent | None = None

    def __post_init__(self):
        assert type(self.features) == list


@dataclass(slots=True, frozen=True)
class Vault:
    """Vault core data.

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

    #: 8 - 18
    denomination_token_decimals: int

    #: Share token address
    #:
    #: Differs for multitoken vaults from vaults address.
    share_token_address: NonChecksummedAddress

    #: "MyVault1"
    share_token_symbol: str

    #: 8 - 18
    share_token_decimals: int

    #: Protocol human readable name
    protocol_name: str

    #: Protocol slug for this vault
    protocol_slug: str

    #: Vault name
    name: str

    #: Vault share token symbol
    token_symbol: str

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

    def __post_init__(self):
        assert self.name, "Vault instance requires name"
        # assert self.token_symbol, "Vault instance requires symbol"

    def is_4626(self) -> bool:
        return ERC4626Feature.erc_4626 in self.features

    def export_as_trading_pair(self) -> dict:
        """EXport data of this vault as compatible for a trading pair.

        - Vaults can be modelled as trading pairs

        :return:
            Trading pair data.

            :py:class:`tradingstrategy.pair.DEXPair` compatible dict.
        """

        assert self.name
        assert "unknown" not in self.name

        metadata = VaultMetadata(
            vault_name=self.name,
            features=list(self.features),
            protocol_slug=self.protocol_slug,
            protocol_name=self.protocol_name,
            performance_fee=self.performance_fee,
            management_fee=self.management_fee,
        )

        return {
            "pair_id": _derive_pair_id(self),
            "pair_slug": _derive_pair_slug(self),
            "exchange_id": _derive_exchange_id(self),
            "address": self.vault_address,
            "token0_address": self.denomination_token_address.lower(),
            "token0_symbol": self.denomination_token_symbol,
            "token0_decimals": self.denomination_token_decimals,
            "token1_address": self.share_token_address.lower(),
            "token1_symbol": self.share_token_symbol,
            "token1_decimals": self.share_token_decimals,
            "dex_type": ExchangeType.erc_4626_vault,
            "base_token_symbol": self.share_token_symbol,
            "quote_token_symbol": self.denomination_token_symbol,
            "exchange_slug": self.protocol_slug,
            "exchange_name": self.name,
            "fee": 0,
            "chain_id": self.chain_id,
            "buy_volume_all_time": 0,
            "token_metadata": metadata,
        }

    def export_as_exchange(self) -> dict:
        """EXport data of this vault as compatible for an exchange.

        - Vaults can be modelled as trading pairs
        - Trading pair needs an exchange
        - We generate exchange entries based on the protocol of the vault

        :return:
            Trading pair data.

            :py:class:`tradingstrategy.pair.DEXPair` compatible dict.
        """
        return {
            "chain_id": self.chain_id,
            "chain_slug": self.chain_id.get_slug(),
            "exchange_id": _derive_exchange_id(self),
            "exchange_slug": self.protocol_slug,
            "name": self.protocol_name,
            "address": "0x0000000000000000000000000000000000000000",
            "exchange_type": ExchangeType.erc_4626_vault,
            "pair_count": 0,
        }

    @classmethod
    def get_pandas_schema(cls) -> dict[str, Any]:
        """Get Pandas schema types."""
        import pandas as pd
        dtype_schema = {
            "pair_id": pd.Int64Dtype(),
            "pair_slug": str,
            "exchange_id": pd.Int64Dtype(),
            "address": str,
            "token0_address": str,
            "token0_symbol": str,
            "token0_decimals": pd.Int64Dtype(),  # Using nullable integer type
            "token1_address": str,
            "token1_symbol": str,
            "token1_decimals": pd.Int64Dtype(),
            "dex_type": str,  # Could use CategoricalDtype if ExchangeType values are known
            "base_token_symbol": str,
            "quote_token_symbol": str,
            "exchange_slug": str,
            "exchange_name": str,
            "fee": pd.Float32Dtype(),
            "chain_id": pd.Int32Dtype(),
            "buy_volume_all_time": pd.Float64Dtype(),
            "token_metadata": object  # Complex object, keeping as object type
        }
        return dtype_schema

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

    def export_all_vaults(self) -> Iterable[Vault]:
        return self.vaults

    def limit_to_single(self, chain_id: ChainId, address: str):
        """Drop all but single vault entry."""
        self.vaults = [vault for vault in self.vaults if vault.chain_id == chain_id and vault.vault_address == address]
        assert len(self.vaults) == 1, f"Expected single vault, got {len(self.vaults)}"

    def limit_to_vaults(self, vaults: list[tuple[ChainId, NonChecksummedAddress]]):
        """Drop all but designednated vault entries."""
        assert all(type(v) in (tuple, list) and isinstance(v[0], ChainId) and v[1].startswith("0x") for v in vaults), f"Bad vault descriptors: {vaults}"
        vaults = set(vaults)
        self.vaults = [vault for vault in self.vaults if (vault.chain_id, vault.vault_address) in vaults]
        assert len(self.vaults) == len(vaults), f"Expected {len(vaults)} vault, got {len(self.vaults)}. Maybe some vault data is mismatch, missing?"


def _derive_pair_id(vault: Vault) -> int:
    """Derive a pair id from the vault address."""
    return _derive_pair_id_from_address(vault.vault_address)


def _derive_pair_id_from_address(address: NonChecksummedAddress) -> int:
    """Derive a pair id from the vault address."""
    id = SPECIAL_PAIR_ID_RANGE + int(address, 16) % (2**24)
    assert id < _js_max_safe_int
    return id


def _derive_exchange_id(vault: Vault) -> int:
    """Derive a exchange id from the vault address."""
    id = SPECIAL_PAIR_ID_RANGE + abs(hash(vault.protocol_slug)) % (2**24)
    assert id < _js_max_safe_int
    return id


def _derive_pair_slug(vault: Vault) -> str:
    """Derive a pair slug from the vault address."""
    return vault.name.lower().replace(" ", "-")

_js_max_safe_int = 2**53 - 1  # 9007199254740991