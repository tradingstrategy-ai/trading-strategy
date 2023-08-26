from enum import Enum
from datetime import datetime

from dataclasses import dataclass, field
from typing import TypeAlias, Tuple, Collection, Iterator, Dict

from dataclasses_json import dataclass_json, config

import pandas as pd

from tradingstrategy.chain import ChainId
from tradingstrategy.token import Token
from tradingstrategy.types import UNIXTimestamp, PrimaryKey, TokenSymbol, Slug, NonChecksummedAddress
from tradingstrategy.utils.groupeduniverse import PairGroupedUniverse


class LendingProtocolType(str, Enum):
    """Supported lending protocols."""
    aave_v3 = "aave_v3"


class LendingCandleType(str, Enum):
    """What kind of lending price feeds we have."""
    stable_borrow_apr = "stable_borrow_apr"
    variable_borrow_apr = "variable_borrow_apr"
    supply_apr = "supply_apr"


class UnknownLendingReserve(Exception):
    """Does not know about this lending reserve."""



@dataclass_json
@dataclass
class LendingReserveAdditionalDetails:
    """Additional details for a lending reserve."""

    #: Latest Loan-To-Value ratio
    ltv: float | None = None

    #: Latest liquidation threshold
    liquidation_threshold: float | None = None


@dataclass_json
@dataclass
class LendingReserve:
    """Describe data for one lending reserve.

    - This is mostly modelled by how lending on Aave works

    - When you lend and asset you will receive a correcting ponding amount of
      aToken in return

    - aToken ERC-20 contract has dynamic balanceOf() function that tells
      you how much principal + interest you have gained
    """

    #: Primary key to identity the lending reserve
    #: Use lending reserve universe to map this to chain id and a smart contract address
    reserve_id: PrimaryKey

    #: The slug of this lending reserve
    #:
    #: E.g. `aave-v3`.
    #:
    reserve_slug: Slug

    #: Which lending protocol this asset is for
    protocol_slug: LendingProtocolType

    #: The id on which chain this lending reserve is deployed
    chain_id: ChainId

    #: The slug on which chain this lending reserve is deployed.
    #:
    #: Needed for website URL linking.
    #:
    #: E.g. `polygon`.
    #:
    chain_slug: Slug

    #: The internal ID of this asset, this might be changed
    asset_id: PrimaryKey

    #: The asset name of this lending reserve
    asset_name: str

    #: The asset symbol of this lending reserve
    asset_symbol: TokenSymbol

    #: The ERC-20 address of the underlying asset
    asset_address: NonChecksummedAddress

    #: Number of decimals
    asset_decimals: int

    #: The internal ID of this the aToken, this might be changed
    atoken_id: PrimaryKey

    #: The aToken symbol of this lending reserve
    atoken_symbol: TokenSymbol

    #: The ERC-20 address of the aToken
    atoken_address: NonChecksummedAddress

    #: The decimals of the aToken .
    #:
    #: Should be always the same as :py:attr:`asset_decimals`
    #:
    atoken_decimals: int

    #: The internal ID of this the vToken, this might be changed
    vtoken_id: PrimaryKey = field(metadata=config(field_name="variable_debt_token_id"))

    #: The vToken symbol of this lending reserve
    vtoken_symbol: TokenSymbol = field(metadata=config(field_name="variable_debt_token_symbol"))

    #: The ERC-20 address of the vToken
    vtoken_address: NonChecksummedAddress = field(metadata=config(field_name="variable_debt_token_address"))

    #: The decimals of the aToken .
    vtoken_decimals: int = field(metadata=config(field_name="variable_debt_token_decimals"))

    #: Other details like latest LTV ratio or liquidation threshold
    additional_details: LendingReserveAdditionalDetails

    def __eq__(self, other: "LendingReserve") -> bool:
        assert isinstance(other, LendingReserve)
        return self.chain_id == other.chain_id and self.protocol_slug == other.protocol_slug and self.asset_address == other.asset_address

    def __hash__(self):
        return hash((self.chain_id, self.protocol_slug, self.asset_address))

    def __repr__(self):
        return f"<LendingReserve {self.chain_id.name} {self.protocol_slug.name} {self.asset_symbol}>"
    
    def get_asset(self) -> Token:
        """Return description for the underlying asset."""
        return Token(
            self.chain_id,
            self.asset_symbol,
            self.asset_address,
            self.asset_decimals,
        )

    def get_atoken(self) -> Token:
        """Return description for aToken."""
        return Token(
            self.chain_id,
            self.atoken_symbol,
            self.atoken_address,
            self.atoken_decimals,
        )
    
    def get_vtoken(self) -> Token:
        """Return description for vToken (variable debt token)."""
        return Token(
            self.chain_id,
            self.vtoken_symbol,
            self.vtoken_address,
            self.vtoken_decimals,
        )

#: How to symbolically identify a lending reserve.
#:
#: Used in human written code instead of unreadable smart contract addresses.
#:
#: - Chain id
#: - Lending protocol type
#: - Reserve token symbol
#: - (Optional) smart contract address
#:
#: Example: `(ChainId.polygon, LendingProtocolType.aave_v3, "USDC")`
#:
#: If there are multiple reserves with the same token, the fourth
#: parameter is a smart contract address that distinguishes these.
#: It is currently not used.
#:
#: Note that the underlying LendingReserve internal ids may change and slugs,
#: only smart contract addresses stay stable.
#:
LendingReserveDescription: TypeAlias = Tuple[ChainId, LendingProtocolType, TokenSymbol] | \
                                       Tuple[ChainId, LendingProtocolType, TokenSymbol, NonChecksummedAddress]



@dataclass_json
@dataclass
class LendingReserveUniverse:
    """Lending reserve universe contains metadata of all lending pools you can access.

    You can use reserve universe as a map to resolve symbolic information
    to the data primary keys.

    For the usage see :py:meth:`resolve_lending_reserve`.
    """

    #: Reserve ID -> Reserve data mapping
    reserves: dict[PrimaryKey, LendingReserve]

    def __repr__(self):
        return f"<LendingReserveUniverse with {len(self.reserves)} reserves>"

    def get_size(self) -> int:
        """How many reserves we have."""
        return len(self.reserves)

    def iter_reserves(self) -> Iterator[LendingReserve]:
        return self.reserves.values()

    def get_reserve_by_id(self, reserve_id: PrimaryKey) -> LendingReserve | None:
        return self.reserves.get(reserve_id)

    def get_reserve_by_symbol_and_chain(
            self,
            token_symbol: TokenSymbol,
            chain_id: ChainId,
    ) -> LendingReserve | None:
        for reserve in self.reserves.values():
            if reserve.asset_symbol == token_symbol and reserve.chain_id == chain_id:
                return reserve
        return None

    def get_by_chain_and_address(
            self,
            chain_id: ChainId,
            asset_address: NonChecksummedAddress,
    ) -> LendingReserve:
        """Get a lending reserve by persistent data.

        :raise UnknownLendingReserve:
            If we do not have data available.
        """
        for reserve in self.reserves.values():
            if reserve.asset_address == asset_address and reserve.chain_id == chain_id:
                return reserve

        raise UnknownLendingReserve(f"Could not find lending reserve {chain_id}: {asset_address}. We have {len(self.reserves)} reserves loaded.")

    def limit(self, reserve_descriptions: Collection[LendingReserveDescription]) -> "LendingReserveUniverse":
        """Remove all lending reverses that are not on the whitelist.

        Used to reduce the lending reserve universe to wanted tradeable assets
        after loading is done.
        """
        new_reserves = {}

        for d in reserve_descriptions:
            r = self.resolve_lending_reserve(d)
            new_reserves[r.reserve_id] = r

        return LendingReserveUniverse(new_reserves)

    def resolve_lending_reserve(self, reserve_decription: LendingReserveDescription) -> LendingReserve:
        """Looks up a lending reserve by a data match.

        Example:

        .. code-block:: python

            usdt_reserve = universe.resolve_lending_reserve(
                (ChainId.polygon,
                LendingProtocolType.aave_v3,
                "USDT")
            )
            assert isinstance(usdt_reserve, LendingReserve)
            assert usdt_reserve.asset_address == '0xc2132d05d31c914a87c6611c10748aeb04b58e8f'
            assert usdt_reserve.asset_symbol == "USDT"
            assert usdt_reserve.asset_name == '(PoS) Tether USD'
            assert usdt_reserve.asset_decimals == 6
            assert usdt_reserve.atoken_symbol == "aPolUSDT"
            assert usdt_reserve.atoken_decimals == 6

        :param reserve_decription:
            Human-readable tuple to resolve the lending reserve.

        :return:
            Metadata for this lending reserve

        :raise UnknownLendingReserve:
            If the loaded data does not contain the reserve
        """

        assert type(reserve_decription) == tuple, f"Lending reserve must be described as tuple, got {reserve_decription}"
        chain_id, slug, symbol, *optional = reserve_decription

        # Validate hard-coded inputs
        assert isinstance(chain_id, ChainId), f"Got {chain_id}"
        assert isinstance(slug, LendingProtocolType), f"Got {slug}"

        assert not optional, "Unsupported"

        for reserve in self.reserves.values():
            if reserve.chain_id == chain_id and \
                    reserve.protocol_slug == slug and \
                    reserve.asset_symbol == symbol:
                return reserve

        raise UnknownLendingReserve(f"Could not find lending reserve {reserve_decription}. We have {len(self.reserves)} reserves loaded.")


@dataclass_json
@dataclass
class LendingCandle:
    """Data structure presenting one OHLC lending candle"""

    #: Primary key to identity the trading pair
    #: Use lending reserve universe to map this to chain id and token symbol.
    #:
    #: See :py:class:`LendingReserveUniverse` how to look up this value.
    #:
    reserve_id: PrimaryKey

    #: Open timestamp for this candle.
    #: Note that the close timestamp you need to supply yourself based on the context.
    timestamp: UNIXTimestamp

    #: OHLC core data
    open: float

    #: OHLC core data
    close: float

    #: OHLC core data
    high: float

    #: OHLC core data
    low: float

    #: Schema definition for :py:class:`pd.DataFrame:
    #:
    #: Defines Pandas datatypes for columns in our candle data format.
    #: Useful e.g. when we are manipulating JSON/hand-written data.
    #:
    DATAFRAME_FIELDS = dict([
        ("reserve_id", "int"),
        ("timestamp", "datetime64[s]"),
        ("open", "float"),
        ("close", "float"),
        ("high", "float"),
        ("low", "float"),
    ])

    def __repr__(self):
        human_timestamp = datetime.utcfromtimestamp(self.timestamp)
        return f"@{human_timestamp} O:{self.open} H:{self.high} L:{self.low} C:{self.close}"

    @classmethod
    def convert_web_candles_to_dataframe(cls, web_candles: list[dict]) -> pd.DataFrame:
        """Return Pandas dataframe presenting candle data."""

        df = pd.DataFrame(web_candles)
        df = df.rename(columns={
            "ts": "timestamp",
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
        })
        df = df.astype(cls.DATAFRAME_FIELDS)

        # Convert unix timestamps to Pandas
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Assume candles are always indexed by their timestamp
        df.set_index("timestamp", inplace=True, drop=True)

        return df


class LendingMetricUniverse(PairGroupedUniverse):
    """Single metric for multiple lending reserves.

    E.g. supply APR for both USDC and USDT.
    Internally handled as GroupBy :py:class:`pd.DataFrame`.

    This is designed so that you can easily develop strategies
    that access lending rates of multiple reserves.

    See also :py:class:`LendingCandleUniverse`.

    To access the data see

    - :py:meth:`tradingstrategy.utils.groupeduniverse.PairGroupedUniverse.get_single_pair_data`

    - :py:meth:`tradingstrategy.utils.groupeduniverse.PairGroupedUniverse.get_samples_by_pair`

    .. note ::

        No forward will enabled yet similar to :py:class:`tradingstrategy.candle.CandleUniverse`.

    """
    def __init__(self, df: pd.DataFrame, reserves: LendingReserveUniverse):
        """
        :param df:
            Output from client lending reserve dat aload

        :param reserves:
            Map of reserve metadata that helps us to resolve symbolic information.

        """
        self.reserves = reserves
        # Create a GroupBy universe from raw loaded lending reserve data
        # We do not need fix any wicks, because lending rates are not subject
        # to similar manipulation as DEX prices
        return super().__init__(
            df,
            primary_key_column="reserve_id",
            fix_wick_threshold=None,
        )

    def get_rates_by_reserve(self, reserve_description: LendingReserveDescription) -> pd.DataFrame:
        """Get all historical rates for a single pair.

        Example:

        .. code-block:: python

            rates = dataset.lending_candles.supply_apr.get_rates_by_reserve(
                (ChainId.polygon, LendingProtocolType.aave_v3, "USDC")
            )
            assert rates["open"][pd.Timestamp("2023-01-01")] == pytest.approx(1.836242)
            assert rates["close"][pd.Timestamp("2023-01-01")] == pytest.approx(1.780513)
        """
        reserve = self.reserves.resolve_lending_reserve(reserve_description)
        return self.get_samples_by_pair(reserve.reserve_id)

    def get_rates_by_id(self, reserve_id: PrimaryKey) -> pd.DataFrame:
        """Return lending rates for for a particular reserve."""
        return self.get_samples_by_pair(reserve_id)


@dataclass
class LendingCandleUniverse:
    """Multiple metrics for all lending reserves.

    Track both lending and borrow rates, so it is
    easy to do credit supply, shorting and longing in the same trading strategy.

    See also :py:class:`LendingMetricUniverse`.

    Example:

    .. code-block:: python

        universe = client.fetch_lending_reserve_universe()

        usdt_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDT")
        usdc_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDC")

        limited_universe = universe.limit([usdt_desc, usdc_desc])

        usdt_reserve = limited_universe.resolve_lending_reserve(usdt_desc)
        usdc_reserve = limited_universe.resolve_lending_reserve(usdc_desc)

        lending_candle_type_map = client.fetch_lending_candles_for_universe(
            limited_universe,
            TimeBucket.d1,
            start_time=pd.Timestamp("2023-01-01"),
            end_time=pd.Timestamp("2023-02-01"),
        )

        universe = LendingCandleUniverse(lending_candle_type_map)

        # Read all data for a single reserve
        usdc_variable_borrow = universe.variable_borrow_apr.get_samples_by_pair(usdc_reserve.reserve_id)

        #            reserve_id      open      high       low     close  timestamp
        # timestamp
        # 2023-01-01           3  1.836242  1.839224  1.780513  1.780513 2023-01-01

        assert usdc_variable_borrow["open"][pd.Timestamp("2023-01-01")] == pytest.approx(1.836242)
        assert usdc_variable_borrow["close"][pd.Timestamp("2023-01-01")] == pytest.approx(1.780513)

        # Read data for multiple reserves for a time range

        #             reserve_id      open      high       low     close  timestamp
        # timestamp
        # 2023-01-05           6  2.814886  2.929328  2.813202  2.867843 2023-01-05
        # 2023-01-06           6  2.868013  2.928622  2.829608  2.866523 2023-01-06

        start = pd.Timestamp("2023-01-05")
        end = pd.Timestamp("2023-01-06")
        iterator = universe.supply_apr.iterate_samples_by_pair_range(start, end)
        for reserve_id, supply_apr in iterator:
            # Read supply apr only for USDT
            if reserve_id == usdt_reserve.reserve_id:
                assert len(supply_apr) == 2  # 2 days
                assert supply_apr["open"][pd.Timestamp("2023-01-05")] == pytest.approx(2.814886)
                assert supply_apr["close"][pd.Timestamp("2023-01-06")] == pytest.approx(2.866523)
    """

    stable_borrow_apr: LendingMetricUniverse | None = None
    variable_borrow_apr: LendingMetricUniverse | None = None
    supply_apr: LendingMetricUniverse | None = None

    def __init__(self, candle_type_dfs: Dict[LendingCandleType, pd.DataFrame], lending_reserve_universe: LendingReserveUniverse):
        """Create the lending candles universe.

        :param candle_type_dfs:
            Different lending metrics.

            Result from :py:meth:`tradingstrategy.client.Client.fetch_lending_candles_for_universe`.
        """

        if LendingCandleType.stable_borrow_apr in candle_type_dfs:
            self.stable_borrow_apr = LendingMetricUniverse(candle_type_dfs[LendingCandleType.stable_borrow_apr], lending_reserve_universe)

        if LendingCandleType.variable_borrow_apr in candle_type_dfs:
            self.variable_borrow_apr = LendingMetricUniverse(candle_type_dfs[LendingCandleType.variable_borrow_apr], lending_reserve_universe)

        if LendingCandleType.supply_apr in candle_type_dfs:
            self.supply_apr = LendingMetricUniverse(candle_type_dfs[LendingCandleType.supply_apr], lending_reserve_universe)

    @property
    def lending_reserves(self) -> LendingReserveUniverse:
        """Get the lending reserve universe.

        It is paired with each metric.
        """
        for metric in (self.stable_borrow_apr, self.variable_borrow_apr, self.supply_apr):
            if metric:
                return metric.reserves

        raise AssertionError("Empty LendingCandlesUniverse")