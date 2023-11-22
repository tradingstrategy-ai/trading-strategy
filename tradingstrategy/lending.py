"""Lending rates and lending protocols.

Data structures for understanding and using lending rates
like supply APR and borrow APR across various DeFi lending protocols.

See :py:class:`LendingReserveUniverse` on how to load the data.
"""
import warnings
from _decimal import Decimal
from enum import Enum
import datetime

from dataclasses import dataclass, field
from typing import TypeAlias, Tuple, Collection, Iterator, Dict, Set

from dataclasses_json import dataclass_json, config

import pandas as pd

from tradingstrategy.chain import ChainId
from tradingstrategy.token import Token
from tradingstrategy.types import UNIXTimestamp, PrimaryKey, TokenSymbol, Slug, NonChecksummedAddress, URL
from tradingstrategy.utils.groupeduniverse import PairGroupedUniverse

from eth_defi.aave_v3.rates import SECONDS_PER_YEAR


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


class NoLendingData(Exception):
    """Lending data missing for asked period.

    Reserve was likely not active yet.
    """


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
        return f"<LendingReserve #{self.reserve_id} for asset {self.asset_symbol} ({self.asset_address}) in protocol {self.protocol_slug.name} on {self.chain_id.get_name()} >"
    
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

    def get_link(self) -> URL:
        """Get the market data page link"""
        return f"https://tradingstrategy.ai/trading-view/{self.chain_id.get_slug()}/lending/{self.protocol_slug}/{self.asset_symbol.lower()}"

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

    Example how to create:

    .. code-block:: python

        exchange_universe = client.fetch_exchange_universe()

        quote_tokens = {
            "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",  # USDC polygon
            "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",  # USDT polygon
        }

        pairs_df = client.fetch_pair_universe().to_pandas()

        # Find out all volatile pairs traded against USDC and USDT on Polygon
        pairs_df = filter_for_chain(pairs_df, ChainId.polygon)
        pairs_df = filter_for_stablecoins(pairs_df, StablecoinFilteringMode.only_volatile_pairs)
        pairs_df = filter_for_quote_tokens(pairs_df, quote_tokens)

        # Create lending universe and trading universe with the cross section of
        # - Available assets in the lending protocols
        # - Asset we can trade
        lending_reserves = client.fetch_lending_reserve_universe()
        lending_only_pairs_df = filter_for_base_tokens(pairs_df, lending_reserves.get_asset_addresses())

        pair_universe = PandasPairUniverse(lending_only_pairs_df, exchange_universe=exchange_universe)

        # Lending reserves have around ~320 individual trading pairs on Polygon across different DEXes
        assert 1 < pair_universe.get_count() < 1_000

        eth_usdc = pair_universe.get_pair_by_human_description((ChainId.polygon, "uniswap-v3", "WETH", "USDC"))
        matic_usdc = pair_universe.get_pair_by_human_description((ChainId.polygon, "uniswap-v3", "WMATIC", "USDC"))

        # Random token not in the Aave v3 supported reserves
        full_pair_universe =  PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)
        bob_usdc = full_pair_universe.get_pair_by_human_description((ChainId.polygon, "uniswap-v3", "BOB", "USDC"))

        assert lending_reserves.can_leverage(eth_usdc.get_base_token())
        assert lending_reserves.can_leverage(matic_usdc.get_base_token())
        assert not lending_reserves.can_leverage(bob_usdc.get_base_token())

    """

    #: Reserve ID -> Reserve data mapping
    reserves: dict[PrimaryKey, LendingReserve]

    def __repr__(self):
        return f"<LendingReserveUniverse with {len(self.reserves)} reserves>"

    def get_count(self) -> int:
        """How many reserves we have.

        :return:
            Number of lending reserves in the trading universe
        """
        return len(self.reserves)

    def iterate_reserves(self) -> Iterator[LendingReserve]:
        return self.reserves.values()

    def get_reserve_by_id(self, reserve_id: PrimaryKey) -> LendingReserve | None:
        return self.reserves.get(reserve_id)

    def get_reserve_by_symbol_and_chain(
            self,
            token_symbol: TokenSymbol,
            chain_id: ChainId,
    ) -> LendingReserve | None:
        warnings.warn("get_reserve_by_symbol_and_chain() has been deprecated in the favour of get_by_chain_and_symbol()", DeprecationWarning, stacklevel=2)
        return self.get_by_chain_and_symbol(chain_id, token_symbol)

    def get_by_chain_and_symbol(
            self,
            chain_id: ChainId,
            token_symbol: TokenSymbol,
    ) -> LendingReserve:
        """Fetch a specific lending reserve.

        :raise UnknownLendingReserve:
            If we do not have data available.
        """

        assert isinstance(chain_id, ChainId), f"Expected chain_id, got {chain_id.__class__}: {chain_id}"

        for reserve in self.reserves.values():
            if reserve.asset_symbol == token_symbol and reserve.chain_id == chain_id:
                return reserve

        raise UnknownLendingReserve(f"Could not find lending reserve {chain_id}: {token_symbol}. We have {len(self.reserves)} reserves loaded.")

    def get_by_chain_and_address(
            self,
            chain_id: ChainId,
            asset_address: NonChecksummedAddress,
    ) -> LendingReserve:
        """Get a lending reserve by persistent data.

        :raise UnknownLendingReserve:
            If we do not have data available.
        """

        assert isinstance(chain_id, ChainId), f"Got: {chain_id}"

        for reserve in self.reserves.values():
            if reserve.asset_address == asset_address and reserve.chain_id == chain_id:
                return reserve

        raise UnknownLendingReserve(f"Could not find lending reserve on chain {chain_id.get_name()}, reserve token address {asset_address}. We have {len(self.reserves)} reserves loaded.")

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

    def limit_to_chain(self, chain_id: ChainId) -> "LendingReserveUniverse":
        """Drop all lending reserves except ones on a specific chain."""
        assert isinstance(chain_id, ChainId)
        new_reserves = {r.reserve_id: r for r in self.reserves.values() if r.chain_id == chain_id}
        return LendingReserveUniverse(new_reserves)

    def limit_to_assets(self, assets: Set[TokenSymbol]) -> "LendingReserveUniverse":
        """Drop all lending reserves except listed tokens."""
        for a in assets:
            assert type(a) == str
        new_reserves = {r.reserve_id: r for r in self.reserves.values() if r.asset_symbol in assets}

        assert len(assets) == len(new_reserves), f"Could not resolve all assets: {assets}"

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

    def get_asset_addresses(self) -> Set[NonChecksummedAddress]:
        """Get all the token addresses in this dataset.

        A shortcut method.

        :return:
            Set of all assets in all lending reserves.
        """
        return {a.asset_address for a in self.reserves.values()}

    def can_leverage(self, token: Token) -> bool:
        """Can we go short/long on a specific token."""
        try:
            self.get_by_chain_and_address(
                token.chain_id,
                token.address,
            )
            return True
        except UnknownLendingReserve:
            return False

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

    def get_rates_by_reserve(
            self,
            reserve_description: LendingReserveDescription | LendingReserve
    ) -> pd.DataFrame:
        """Get all historical rates for a single pair.

        Example:

        .. code-block:: python

            rates = dataset.lending_candles.supply_apr.get_rates_by_reserve(
                (ChainId.polygon, LendingProtocolType.aave_v3, "USDC")
            )
            assert rates["open"][pd.Timestamp("2023-01-01")] == pytest.approx(1.836242)
            assert rates["close"][pd.Timestamp("2023-01-01")] == pytest.approx(1.780513)
        """
        if isinstance(reserve_description, LendingReserve):
            reserve = reserve_description
        elif type(reserve_description) == tuple:
            reserve = self.reserves.resolve_lending_reserve(reserve_description)
        else:
            raise AssertionError(f"Unknown lending reserve description: {reserve_description}")
        return self.get_samples_by_pair(reserve.reserve_id)

    def get_rates_by_id(self, reserve_id: PrimaryKey) -> pd.DataFrame:
        """Return lending rates for for a particular reserve."""
        return self.get_samples_by_pair(reserve_id)

    def get_single_rate(
        self,
        reserve: LendingReserve,
        when: pd.Timestamp | datetime.datetime,
        data_lag_tolerance: pd.Timedelta,
        kind="close",
    ) -> Tuple[float, pd.Timedelta]:
        """Get a single historical value of a lending rate.

        See :py:meth:`tradingstrategy.utils.groupeduniverse.PairGroupedUniverse.get_single_value`
        for documentation.

        Example:

        .. code-block:: python

            lending_reserves = client.fetch_lending_reserve_universe()

            usdt_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDT")
            usdc_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDC")

            lending_reserves = lending_reserves.limit([usdt_desc, usdc_desc])

            lending_candle_type_map = client.fetch_lending_candles_for_universe(
                lending_reserves,
                TimeBucket.d1,
                start_time=pd.Timestamp("2023-01-01"),
                end_time=pd.Timestamp("2023-02-01"),
            )

            lending_candles = LendingCandleUniverse(lending_candle_type_map, lending_reserves)

            usdc_reserve = lending_reserves.resolve_lending_reserve(usdc_desc)

            rate, lag = lending_candles.variable_borrow_apr.get_single_rate(
                usdc_reserve,
                pd.Timestamp("2023-01-01"),
                data_lag_tolerance=pd.Timedelta(days=1),
                kind="open",
            )
            assert rate == pytest.approx(1.836242)
            assert lag == ZERO_TIMEDELTA

        :return:
            Tuple (lending rate, data lag)

            The lending rate may be 0 if the reserve is special
            and borrowing/lending is disabled.
        """

        assert isinstance(reserve, LendingReserve), f"Got {reserve.__class__}"

        asset_name = reserve.asset_name
        link = reserve.get_link()

        return self.get_single_value(
            reserve.reserve_id,
            when,
            data_lag_tolerance,
            kind,
            asset_name=asset_name,
            link=link,
        )

    def estimate_accrued_interest(
        self,
        reserve: LendingReserveDescription | LendingReserve,
        start: datetime.datetime | pd.Timestamp,
        end: datetime.datetime | pd.Timestamp,
    ) -> Decimal:
        """Estimate how much credit or debt interest we would gain on Aave at a given period.

        Example:

        .. code-block:

            lending_reserves = client.fetch_lending_reserve_universe()

            usdc_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDC")

            lending_reserves = lending_reserves.limit([usdc_desc])

            lending_candle_type_map = client.fetch_lending_candles_for_universe(
                lending_reserves,
                TimeBucket.d1,
                start_time=pd.Timestamp("2022-09-01"),
                end_time=pd.Timestamp("2023-09-01"),
            )
            lending_candles = LendingCandleUniverse(lending_candle_type_map, lending_reserves)

            # Estimate borrow cost
            borrow_interest_multiplier = lending_candles.variable_borrow_apr.estimate_accrued_interest(
                usdc_desc,
                start=pd.Timestamp("2022-09-01"),
                end=pd.Timestamp("2023-09-01"),
            )
            assert borrow_interest_multiplier == pytest.approx(Decimal('1.028597760665127969909038441'))

            # Estimate borrow cost
            supply_interest_multiplier = lending_candles.supply_apr.estimate_accrued_interest(
                usdc_desc,
                start=pd.Timestamp("2022-09-01"),
                end=pd.Timestamp("2023-09-01"),
            )
            assert supply_interest_multiplier == pytest.approx(Decimal('1.017786465640168974688961612'))

        :param reserve:
            Asset we are interested in.

        :param start:
            Start of the period

        :param end:
            End of the period

        :return:
            Interest multiplier.

            Multiply the starting balance with this number to get the interest applied balance at ``end``.

            1.0 = no interest.
        """

        if isinstance(start, datetime.datetime):
            start = pd.Timestamp(start)

        if isinstance(end, datetime.datetime):
            end = pd.Timestamp(end)

        assert isinstance(start, pd.Timestamp), f"Not a timestamp: {start}"
        assert isinstance(end, pd.Timestamp), f"Not a timestamp: {end}"

        assert start <= end

        df = self.get_rates_by_reserve(reserve)

        # TODO: Can we use index here to speed up?
        candles = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]

        if len(candles) == 0:
            raise NoLendingData(f"No lending data for {reserve}, {start} - {end}")

        # get average APR from high and low
        avg = candles[["high", "low"]].mean(axis=1)
        avg_apr = Decimal(avg.mean() / 100)

        duration = Decimal((end - start).total_seconds())
        accrued_interest_estimation = 1 + (1 * avg_apr) * duration / SECONDS_PER_YEAR  # Use Aave v3 seconds per year

        assert accrued_interest_estimation >= 1, f"Aave interest cannot be negative: {accrued_interest_estimation}"

        return accrued_interest_estimation


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


def convert_interest_rates_to_lending_candle_type_map(*args):
    """Convert lending and supply interest rates for all assets to a single lending_candle_type map.
    
    :param *args:
        List of dictionaries for each reserve. Each dictionary must have 3 keys/value pairs of the form:
        1. reserve_id: reserve id
        2. lending_data: pandas Series of lending rates
        3. supply_data: pandas Series of suppy rates

        Note that lending_data and supply_data should start and end at the same dates.

    :return: Dictionary of lending_candle_type_map
    """

    data = []
    for dictionary in args:
        assert set(dictionary.keys()) == {"reserve_id", "lending_data", "supply_data"}

        reserve_id = dictionary["reserve_id"]
        _lending_data = dictionary["lending_data"]
        _supply_data = dictionary["supply_data"]
        
        assert len(_lending_data) == len(_supply_data), "Lending data and supply data must have the same length"
        assert isinstance(_lending_data.index, pd.DatetimeIndex), "Index must be a DatetimeIndex"
        assert _lending_data.index.equals(_supply_data.index), "Lending data and supply data must have the same index"

        data.extend(zip(dictionary["lending_data"], dictionary["supply_data"], [reserve_id] * len(_lending_data), _lending_data.index))

    lending_candle_type_map = {
        LendingCandleType.variable_borrow_apr: pd.DataFrame({
            "open": [_data[0] for _data in data],
            "close": [_data[0] for _data in data],
            "high": [_data[0] for _data in data],
            "low": [_data[0] for _data in data],
            "timestamp": [_data[3] for _data in data],
            "reserve_id": [_data[2] for _data in data],
        }),
        LendingCandleType.supply_apr: pd.DataFrame({
            "open": [_data[1] for _data in data],
            "close": [_data[1] for _data in data],
            "high": [_data[1] for _data in data],
            "low": [_data[1] for _data in data],
            "timestamp": [_data[3] for _data in data],
            "reserve_id": [_data[2] for _data in data],
        }),
    }

    return lending_candle_type_map
