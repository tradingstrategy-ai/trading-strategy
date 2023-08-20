from enum import Enum
from datetime import datetime

from dataclasses import dataclass
from typing import TypeAlias, Tuple

from dataclasses_json import dataclass_json

import pandas as pd

from tradingstrategy.chain import ChainId
from tradingstrategy.types import UNIXTimestamp, PrimaryKey, TokenSymbol, Slug, NonChecksummedAddress


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

    #: The ERC-20 address of the aToken.
    #:
    #: Should be always the same as :py:attr:`asset_decimals`
    #:
    atoken_decimals: int

    def __repr__(self):
        return f"<LendingReserve {self.chain_id.name} {self.protocol_slug.name} {self.asset_symbol}>"


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

    def get_reserve_by_id(self, reserve_id: PrimaryKey) -> LendingReserve | None:
        return self.reserves.get(reserve_id)

    def get_reserve_by_symbol_and_chain(
            self,
            token_symbol: TokenSymbol,
            chain_id: ChainId,
    ) -> LendingReserve | None:
        """TODO: this is the slow method to deal with this, improve later
        """
        for reserve in self.reserves.values():
            if reserve.asset_symbol == token_symbol and reserve.chain_id == chain_id:
                return reserve
        return None

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
