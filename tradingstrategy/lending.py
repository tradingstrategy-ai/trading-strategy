import enum
from datetime import datetime

from dataclasses import dataclass
from dataclasses_json import dataclass_json

import pandas as pd

from tradingstrategy.chain import ChainId
from tradingstrategy.types import (
    NonChecksummedAddress, BlockNumber, UNIXTimestamp, BasisPoint, PrimaryKey, 
    Percent, USDollarAmount,
)


class LendingProtocolType(str, enum.Enum):
    aave_v3 = "aave_v3"
    


@dataclass_json
@dataclass
class LendingReserve:
    #: Primary key to identity the lending reserve
    #: Use lending reserve universe to map this to chain id and a smart contract address
    reserve_id: PrimaryKey
    reserve_slug: str

    protocol_slug: str

    #: The chain id on which chain this pair is trading. 1 for Ethereum.
    chain_id: int
    chain_slug: str

    #: Smart contract address for the pair.
    #: In the case of Uniswap this is the pair (pool) address.
    # address: NonChecksummedAddress

    asset_id: int
    asset_name: str

    #: ERC-20 contracts are not guaranteed to have this data.
    asset_symbol: str

    # atoken_id: int
    # atoken_address: NonChecksummedAddress
    # stable_debt_token_id: int
    # stable_debt_token_address: NonChecksummedAddress
    # variable_debt_token_id: int
    # variable_debt_token_address: NonChecksummedAddress
    # interest_rate_strategy_address: NonChecksummedAddress



@dataclass_json
@dataclass
class LendingReserveUniverse:
    #: Reserve ID -> Reserve data mapping
    reserves: dict[PrimaryKey, LendingReserve]

    def get_reserve_by_id(self, reserve_id: PrimaryKey) -> LendingReserve | None:
        return self.reserves[reserve_id]
    
    def get_reserve_by_symbol_and_chain(
        self,
        token_symbol: str,
        chain_id: int,
    ) -> LendingReserve | None:
        """TODO: this is the slow method to deal with this, improve later
        """
        for reserve in self.reserves.values():
            if reserve.asset_symbol == token_symbol and reserve.chain_id == chain_id:
                return reserve
        return None


class LendingCandleTypes(str, enum.Enum):
    """The supported properties the reserves can be sorted by."""
    stable_borrow_apr = enum.auto()
    variable_borrow_apr = enum.auto()
    supply_apr = enum.auto()



@dataclass_json
@dataclass
class LendingCandle:
    """Data structure presenting one OHLC lending candle"""

    #: Primary key to identity the trading pair
    #: Use pair universe to map this to chain id and a smart contract address
    reserve_id: PrimaryKey

    #: Open timestamp for this candle.
    #: Note that the close timestamp you need to supply yourself based on the context.
    timestamp: UNIXTimestamp  # UNIX timestamp as seconds

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
    def to_dataframe(cls) -> pd.DataFrame:
        """Return emptry Pandas dataframe presenting candle data."""

        df = pd.DataFrame(columns=cls.DATAFRAME_FIELDS.keys())
        return df.astype(cls.DATAFRAME_FIELDS)
