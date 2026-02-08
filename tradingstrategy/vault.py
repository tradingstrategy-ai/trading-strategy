""""Vault data for EIP-4626 and other digital asset management protocols."""
import datetime
from dataclasses import dataclass, field
from typing import Iterable, TypeAlias, Any, Collection

try:
    from eth_defi.erc_4626.core import ERC4626Feature
except ImportError:
    # Spoof for soft imports
    ERC4626Feature: TypeAlias = str

try:
    from eth_defi.research.vault_metrics import PeriodMetrics
except ImportError:
    # Spoof for soft imports
    PeriodMetrics: TypeAlias = Any

from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import ExchangeType
from tradingstrategy.types import Percent, NonChecksummedAddress, TokenSymbol
from tradingstrategy.types import SPECIAL_PAIR_ID_RANGE


@dataclass(slots=True, frozen=True)
class VaultMetadata:
    """Used in pair dataframe for vault specific data.

    .. note::

        The :py:attr:`period_results` field contains structured :py:class:`PeriodMetrics` objects
        and should be preferred over the flat class-level performance fields (e.g., ``cagr``,
        ``volatility``, ``sharpe``). The flat fields are provided for convenience and backward
        compatibility, but ``period_results`` provides more complete and structured data including
        metrics for multiple time periods (1W, 1M, 3M, 6M, 1Y, lifetime).

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

    #: Total lifetime gross returns.
    #:
    #: Calculated from inception to now.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.returns_gross`` for lifetime period instead.
    #:
    lifetime_return: Percent | None = None

    #: Total lifetime net returns (after fees).
    #:
    #: Calculated from inception to now, with management and performance fees deducted.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.returns_net`` for lifetime period instead.
    #:
    lifetime_return_net: Percent | None = None

    #: Compound annual growth rate (gross).
    #:
    #: Annualised return based on lifetime performance.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.cagr_gross`` for lifetime period instead.
    #:
    cagr: Percent | None = None

    #: Compound annual growth rate (net of fees).
    #:
    #: Annualised return based on lifetime performance, with fees deducted.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.cagr_net`` for lifetime period instead.
    #:
    cagr_net: Percent | None = None

    #: 3-month gross returns.
    #:
    #: Absolute return over the last 3 months.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.returns_gross`` for 3M period instead.
    #:
    three_months_return: Percent | None = None

    #: 3-month net returns.
    #:
    #: Absolute return over the last 3 months, with fees deducted.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.returns_net`` for 3M period instead.
    #:
    three_months_return_net: Percent | None = None

    #: 3-month annualised gross returns.
    #:
    #: CAGR calculated from the last 3 months of data.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.cagr_gross`` for 3M period instead.
    #:
    three_months_cagr: Percent | None = None

    #: 3-month annualised net returns.
    #:
    #: CAGR calculated from the last 3 months of data, with fees deducted.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.cagr_net`` for 3M period instead.
    #:
    three_months_cagr_net: Percent | None = None

    #: 3-month volatility (annualised).
    #:
    #: Standard deviation of daily returns, annualised.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.volatility`` for 3M period instead.
    #:
    volatility: Percent | None = None

    #: 3-month Sharpe ratio.
    #:
    #: Risk-adjusted return metric calculated from the last 3 months.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.sharpe`` for 3M period instead.
    #:
    sharpe: float | None = None

    #: Maximum drawdown.
    #:
    #: Largest peak-to-trough decline during the 3-month period.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.max_drawdown`` for 3M period instead.
    #:
    max_drawdown: Percent | None = None

    #: Current total value locked (USD).
    #:
    #: The current assets under management in the vault.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.tvl_end`` instead.
    #:
    tvl: float | None = None

    #: Peak TVL ever (USD).
    #:
    #: The highest TVL the vault has ever reached.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.tvl_high`` for lifetime period instead.
    #:
    tvl_peak: float | None = None

    #: Vault age in years.
    #:
    #: Time since the vault was first seen or deployed.
    age_years: float | None = None

    #: When first price data was recorded.
    #:
    #: Timestamp of the earliest available price data.
    first_updated_at: datetime.datetime | None = None

    #: When last price data was recorded.
    #:
    #: Timestamp of the most recent price data update.
    last_updated_at: datetime.datetime | None = None

    #: Deposit fee.
    #:
    #: Fee charged when depositing into the vault.
    deposit_fee: Percent | None = None

    #: Withdrawal fee.
    #:
    #: Fee charged when withdrawing from the vault.
    withdrawal_fee: Percent | None = None

    #: Lock-up period in days.
    #:
    #: Estimated number of days funds are locked after deposit.
    lockup_days: float | None = None

    #: Risk classification name.
    #:
    #: Human-readable risk level (e.g. "low", "medium", "high", "blacklisted").
    risk_level: str | None = None

    #: Any notes about the vault.
    #:
    #: Additional information or warnings about this vault.
    notes: str | None = None

    #: Reason deposits are closed.
    #:
    #: If deposits are currently not accepted, this explains why.
    deposit_closed_reason: str | None = None

    #: Reason redemptions are closed.
    #:
    #: If redemptions are currently not accepted, this explains why.
    redemption_closed_reason: str | None = None

    #: When deposits will next be open.
    #:
    #: If deposits are currently closed, this indicates when they will reopen.
    deposit_next_open: datetime.datetime | None = None

    #: When redemptions will next be open.
    #:
    #: If redemptions are currently closed, this indicates when they will reopen.
    redemption_next_open: datetime.datetime | None = None

    #: 1-month gross returns.
    #:
    #: Absolute return over the last 1 month.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.returns_gross`` for 1M period instead.
    #:
    one_month_return: Percent | None = None

    #: 1-month net returns.
    #:
    #: Absolute return over the last 1 month, with fees deducted.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.returns_net`` for 1M period instead.
    #:
    one_month_return_net: Percent | None = None

    #: 1-month annualised gross returns.
    #:
    #: CAGR calculated from the last 1 month of data.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.cagr_gross`` for 1M period instead.
    #:
    one_month_cagr: Percent | None = None

    #: 1-month annualised net returns.
    #:
    #: CAGR calculated from the last 1 month of data, with fees deducted.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.cagr_net`` for 1M period instead.
    #:
    one_month_cagr_net: Percent | None = None

    #: URL-friendly vault name slug.
    #:
    #: Used for constructing URLs to the vault page.
    vault_slug: str | None = None

    #: Vault smart contract address.
    #:
    #: The main vault contract address (lowercase, non-checksummed).
    address: NonChecksummedAddress | None = None

    #: Chain name.
    #:
    #: Human-readable blockchain name (e.g., "Ethereum", "Arbitrum").
    chain: str | None = None

    #: Numeric chain ID.
    #:
    #: EVM chain ID (e.g., 1 for Ethereum, 42161 for Arbitrum).
    chain_id: int | None = None

    #: Share token contract address.
    #:
    #: Address of the ERC-20 share token (lowercase, non-checksummed).
    share_token_address: NonChecksummedAddress | None = None

    #: Denomination token contract address.
    #:
    #: Address of the underlying asset token (lowercase, non-checksummed).
    denomination_token_address: NonChecksummedAddress | None = None

    #: Denomination token symbol.
    #:
    #: Symbol of the underlying asset (e.g., "USDC", "WETH").
    denomination: TokenSymbol | None = None

    #: Share token symbol.
    #:
    #: Symbol of the vault's share token.
    share_token: TokenSymbol | None = None

    #: Number of deposit and redemption events.
    #:
    #: Total count of deposit and withdrawal transactions.
    event_count: int | None = None

    #: Current share price.
    #:
    #: Most recent share price in denomination token units.
    last_share_price: float | None = None

    #: Whether the denomination token is stablecoin-like.
    #:
    #: True if the underlying asset is a stablecoin (USDC, USDT, DAI, etc.).
    stablecoinish: bool | None = None

    #: Fee mode classification.
    #:
    #: How fees are structured (e.g., "externalised", "internalised").
    fee_mode: str | None = None

    #: Whether fees are internalised.
    #:
    #: True if fees are taken from the share price rather than charged separately.
    fee_internalised: bool | None = None

    #: Human-readable fee label.
    #:
    #: Formatted fee display (e.g., "2% / 20%" for mgmt/perf fees).
    fee_label: str | None = None

    #: External link to vault.
    #:
    #: URL to the vault's official page or interface.
    link: str | None = None

    #: Link to Trading Strategy vault page.
    #:
    #: URL to the vault's page on tradingstrategy.ai.
    trading_strategy_link: str | None = None

    #: Vault flags.
    #:
    #: Set of warning or status flags for the vault.
    flags: list | None = None

    #: Block number of first price update.
    #:
    #: The blockchain block when first price data was recorded.
    first_updated_block: int | None = None

    #: Block number of last price update.
    #:
    #: The blockchain block when most recent price data was recorded.
    last_updated_block: int | None = None

    #: Start timestamp of 1-month sample period.
    #:
    #: When the 1-month metric calculation period began.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.samples_start_at`` for 1M period instead.
    #:
    one_month_start: datetime.datetime | None = None

    #: End timestamp of 1-month sample period.
    #:
    #: When the 1-month metric calculation period ended.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.samples_end_at`` for 1M period instead.
    #:
    one_month_end: datetime.datetime | None = None

    #: Number of samples in 1-month period.
    #:
    #: Count of data points used for 1-month calculations.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.raw_samples`` for 1M period instead.
    #:
    one_month_samples: int | None = None

    #: Start timestamp of 3-month sample period.
    #:
    #: When the 3-month metric calculation period began.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.samples_start_at`` for 3M period instead.
    #:
    three_months_start: datetime.datetime | None = None

    #: End timestamp of 3-month sample period.
    #:
    #: When the 3-month metric calculation period ended.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.samples_end_at`` for 3M period instead.
    #:
    three_months_end: datetime.datetime | None = None

    #: Number of samples in 3-month period.
    #:
    #: Count of data points used for 3-month calculations.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.raw_samples`` for 3M period instead.
    #:
    three_months_samples: int | None = None

    #: Start timestamp of lifetime sample period.
    #:
    #: When the lifetime metric calculation period began.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.samples_start_at`` for lifetime period instead.
    #:
    lifetime_start: datetime.datetime | None = None

    #: End timestamp of lifetime sample period.
    #:
    #: When the lifetime metric calculation period ended.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.samples_end_at`` for lifetime period instead.
    #:
    lifetime_end: datetime.datetime | None = None

    #: Number of samples in lifetime period.
    #:
    #: Count of data points used for lifetime calculations.
    #:
    #: .. deprecated::
    #:
    #:    Use :py:attr:`period_results` with ``PeriodMetrics.raw_samples`` for lifetime period instead.
    #:
    lifetime_samples: int | None = None

    #: Structured period metrics results.
    #:
    #: List of PeriodMetrics objects for each time period (1W, 1M, 3M, 6M, 1Y, lifetime).
    period_results: list[PeriodMetrics] | None = None

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

    #: Rich metadata including performance metrics, fees, risk level, etc.
    #:
    #: Populated when loading from JSON blob via
    #: :py:func:`tradingstrategy.alternative_data.vault.load_vault_database_with_metadata`
    metadata: "VaultMetadata | None" = None

    def __post_init__(self):
        assert self.name, "Vault instance requires name"
        # assert self.token_symbol, "Vault instance requires symbol"

    def is_4626(self) -> bool:
        return ERC4626Feature.erc_4626 in self.features

    def get_spec(self) -> tuple[ChainId, NonChecksummedAddress]:
        """Get vault spec as (chain_id, address)."""
        return (self.chain_id, self.vault_address.lower())

    def get_metadata(self) -> "VaultMetadata":
        """Get vault metadata.

        Returns stored metadata if available, otherwise constructs minimal metadata
        from the available Vault fields.

        :return:
            VaultMetadata object with available fields populated.
        """
        if self.metadata is not None:
            return self.metadata

        return VaultMetadata(
            vault_name=self.name,
            features=list(self.features),
            protocol_slug=self.protocol_slug,
            protocol_name=self.protocol_name,
            performance_fee=self.performance_fee,
            management_fee=self.management_fee,
            tvl=self.tvl,
            address=self.vault_address,
            chain_id=self.chain_id.value,
            chain=self.chain_id.get_name(),
            share_token_address=self.share_token_address,
            share_token=self.share_token_symbol,
            denomination_token_address=self.denomination_token_address,
            denomination=self.denomination_token_symbol,
            first_updated_at=self.deployed_at,
            last_updated_at=self.denormalised_data_updated_at,
        )

    def export_as_trading_pair(self) -> dict:
        """Export data of this vault as compatible for a trading pair.

        - Vaults can be modelled as trading pairs

        :return:
            Trading pair data.

            :py:class:`tradingstrategy.pair.DEXPair` compatible dict.
        """

        assert self.name
        assert "unknown" not in self.name

        metadata = self.get_metadata()

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

    def __init__(self, vaults: Iterable[Vault]):
        self.vaults: dict[tuple[ChainId, NonChecksummedAddress], Vault] = {v.get_spec(): v for v in vaults}
        assert len(self.vaults) > 0, "Vault universe cannot be empty"
        assert isinstance(next(iter(self.vaults.values())), Vault)

    def get_by_chain_and_name(self, chain_id: ChainId | int, name: str) -> Vault | None:
        """Get vault by chain id and name."""

        if type(chain_id) == int:
            chain_id = ChainId(chain_id)

        assert isinstance(chain_id, ChainId)
        assert type(name) == str
        for vault in self.vaults.values():
            if vault.chain_id == chain_id and vault.name == name:
                return vault
        return None

    def get_by_vault_spec(self, spec: tuple[ChainId | int, NonChecksummedAddress]) -> Vault | None:
        """Get vault by chain id and name."""

        chain_id = spec[0]
        address = spec[1]

        if type(chain_id) == int:
            chain_id = ChainId(chain_id)

        assert isinstance(chain_id, ChainId)
        assert type(address) == str
        assert address.startswith("0x")
        return self.vaults.get((chain_id, address.lower()))

    def get_vault_count(self) -> int:
        """Get number of vaults in the universe."""
        return len(self.vaults)

    def export_all_vaults(self) -> Iterable[Vault]:
        return self.vaults.values()

    def limit_to_single(self, chain_id: ChainId, address: str) -> "VaultUniverse":
        """Drop all but single vault entry."""
        vault_list = [vault for vault in self.vaults.values() if vault.chain_id == chain_id and vault.vault_address == address]
        assert len(vault_list) == 1, f"Expected single vault, got {len(self.vaults)}"
        return VaultUniverse(vault_list)

    def limit_to_vaults(
        self,
        vaults: list[tuple[ChainId, NonChecksummedAddress]],
        check_all_vaults_found: bool = True,
    ) -> "VaultUniverse":
        """Drop all but designednated vault entries.

        :param check_all_vaults_found:
            Check that we have metadata for all vaults in our local files.

            If not set, skip and do not care if some vaults are missing.
        """
        assert all(type(v) in (tuple, list) and isinstance(v[0], ChainId) and v[1].startswith("0x") for v in vaults), f"Bad vault descriptors: {vaults}"
        vaults = set(vaults)

        if check_all_vaults_found:    
            # Check if we have all given vault addresses in our vault universe        
            vault_list = [vault for vault in self.vaults.values() if (vault.chain_id, vault.vault_address) in vaults]
            if len(vault_list) != len(vaults):
                found = {vault.vault_address for vault in vault_list}
                missing_msg = ""
                for v in vaults:
                    if v[1] not in found:
                        missing_msg += f"\n - Missing vault {v[1]} on chain {v[0]}"

                msg= f"Expected {len(vaults)} vault, got {len(self.vaults)}. Maybe some vault data is mismatch, missing?\n"
                raise AssertionError(msg + missing_msg)
        else:
            # Use iterator
            vault_list = (vault for vault in self.vaults.values() if (vault.chain_id, vault.vault_address) in vaults)

        return VaultUniverse(vault_list)

    def limit_to_denomination(
        self,
        denomination_token_symbols: Collection[TokenSymbol],
        check_all_vaults_found: bool = False,
    ) -> "VaultUniverse":
        """Drop all but designednated vault entries.

        :param check_all_vaults_found:
            Check that we have metadata for all vaults in our local files.

            If not set, skip and do not care if some vaults are missing.
        """

        vault_list = []

        if check_all_vaults_found:
            # Check what vaults are not included            
            for vault in self.vaults.values():
                if vault.denomination_token_symbol in denomination_token_symbols:
                    vault_list.append(vault)
                else:
                    raise AssertionError(f"Cannot include vault {vault.name} with denomination {vault.denomination_token_symbol}")
            
        else:
            vault_list = (vault for vault in self.vaults.values() if vault.denomination_token_symbol in denomination_token_symbols)

        return VaultUniverse(vault_list)

    def limit_to_chain(self, chain_id: ChainId | int) -> "VaultUniverse":
        """Drop all but single chain vault entries."""
        if type(chain_id) == int:
            chain_id = ChainId(chain_id)
        assert isinstance(chain_id, ChainId)
        vault_list = (vault for vault in self.vaults.values() if vault.chain_id == chain_id)
        return VaultUniverse(vault_list)

    def iterate_vaults(self) -> Iterable[Vault]:
        """Iterate over all vaults."""
        yield from self.vaults.values()


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