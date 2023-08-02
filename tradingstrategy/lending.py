from enum import Enum
from datetime import datetime

from dataclasses import dataclass
from dataclasses_json import dataclass_json

import pandas as pd

from tradingstrategy.types import UNIXTimestamp, PrimaryKey


class LendingProtocolType(str, Enum):
    aave_v3 = "aave_v3"


class LendingCandleType(str, Enum):
    stable_borrow_apr = "stable_borrow_apr"
    variable_borrow_apr = "variable_borrow_apr"
    supply_apr = "supply_apr"


@dataclass_json
@dataclass
class LendingReserve:
    #: Primary key to identity the lending reserve
    #: Use lending reserve universe to map this to chain id and a smart contract address
    reserve_id: PrimaryKey

    #: The slug of this lending reserve
    reserve_slug: str

    protocol_slug: LendingProtocolType

    #: The id on which chain this lending reserve is deployed
    chain_id: int

    #: The slug on which chain this lending reserve is deployed
    chain_slug: str

    #: The asset ID of this lending reserve
    asset_id: int

    #: The asset name of this lending reserve
    asset_name: str

    #: The asset symbol of this lending reserve
    asset_symbol: str


@dataclass_json
@dataclass
class LendingReserveUniverse:
    #: Reserve ID -> Reserve data mapping
    reserves: dict[PrimaryKey, LendingReserve]

    def get_reserve_by_id(self, reserve_id: PrimaryKey) -> LendingReserve | None:
        return self.reserves.get(reserve_id)
    
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


@dataclass_json
@dataclass
class LendingCandle:
    """Data structure presenting one OHLC lending candle"""

    #: Primary key to identity the trading pair
    #: Use lending reserve universe to map this to chain id and token symbol
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
