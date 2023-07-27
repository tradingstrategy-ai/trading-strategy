import enum

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



# @dataclass_json
# @dataclass
# class LendingCandle:
#     """Data structure presenting one OHLCV trading candle.

#     Based on the :term:`open-high-low-close-volume <OHLCV>` concept.

#     Trading Strategy candles come with additional information available on the top of core OHLCV,
#     as chain analysis has deeper visibility than one would get on traditional exchanges.
#     For example for enhanced attributes see :py:attr:`Candle.buys` (buy count) or
#     :py:attr:`Candle.start_block` (blockchain starting block number of the candle).

#     We also separate "buys" and "sells". Although this separation might not be meaningful
#     on order-book based exchanges, we define "buy" as a DEX swap where quote token (USD, ETH)
#     was swapped into more exotic token (AAVE, SUSHI, etc.)
#     """

#     #: Primary key to identity the trading pair
#     #: Use pair universe to map this to chain id and a smart contract address
#     lending_reserve_id: PrimaryKey

#     #: Open timestamp for this candle.
#     #: Note that the close timestamp you need to supply yourself based on the context.
#     timestamp: UNIXTimestamp  # UNIX timestamp as seconds

#     #: USD exchange rate of the quote token used to
#     #: convert to dollar amounts in this candle.
#     #:
#     #: Note that currently any USD stablecoin (USDC, DAI) is
#     #: assumed to be 1:1 and the candle server cannot
#     #: handle exchange rate difference among stablecoins.
#     #:
#     #: The rate is taken at the beginning of the 1 minute time bucket.
#     #: For other time buckets, the exchange rate is the simple average
#     #: for the duration of the bucket.
#     exchange_rate: float

#     #: OHLC core data
#     open: USDollarAmount

#     #: OHLC core data
#     close: USDollarAmount

#     #: OHLC core data
#     high: USDollarAmount

#     #: OHLC core data
#     low: USDollarAmount

#     #: Number of buys happened during the candle period.
#     #:
#     #: Only avaiable on DEXes where buys and sells can be separaed.
#     buys: int | None

#     #: Number of sells happened during the candle period
#     #:
#     #: Only avaiable on DEXes where buys and sells can be separaed.    
#     sells: int | None

#     #: Trade volume
#     volume: USDollarAmount

#     #: Buy side volume
#     #:
#     #: Swap quote token -> base token volume
#     buy_volume: USDollarAmount | None

#     #: Sell side volume
#     #:
#     #: Swap base token -> quote token volume
#     sell_volume: USDollarAmount | None

#     #: Average trade size
#     avg: USDollarAmount

#     #: The first blockchain block that includes trades that went into this candle.
#     start_block: BlockNumber

#     #: The last blockchain block that includes trades that went into this candle.
#     end_block: BlockNumber

#     #: TODO: Currently disabled to optimise speed
#     #:
#     #: This candle contained bad wicked :py:attr:`high` or :py:attr:`low` data and was filtered out.
#     #:
#     #: See :py:func:`tradingstrategy.utils.groupeduniverse.filter_bad_high_low`.
#     #: These might be natural causes for the bad data. However,
#     #: we do not want to deal with these situations inside a trading strategy.
#     #: Thus, we fix candles with unrealisitc high and low wicks during the
#     #: data loading.
#     #:
#     #: Not set unless the filter has been run on the fetched data.
#     # wick_filtered: Optional[bool] = None,

#     #: Schema definition for :py:class:`pd.DataFrame:
#     #:
#     #: Defines Pandas datatypes for columns in our candle data format.
#     #: Useful e.g. when we are manipulating JSON/hand-written data.
#     #:
#     DATAFRAME_FIELDS = dict([
#         ("pair_id", "int"),
#         ("timestamp", "datetime64[s]"),
#         ("exchange_rate", "float"),
#         ("open", "float"),
#         ("close", "float"),
#         ("high", "float"),
#         ("low", "float"),
#         ("buys", "float"),
#         ("sells", "float"),
#         ("volume", "float"),
#         ("buy_volume", "float"),
#         ("sell_volume", "float"),
#         ("avg", "float"),
#         ("start_block", "int"),
#         ("end_block", "int"),
#     ])

#     def __repr__(self):
#         human_timestamp = datetime.datetime.utcfromtimestamp(self.timestamp)
#         return f"@{human_timestamp} O:{self.open} H:{self.high} L:{self.low} C:{self.close} V:{self.volume} B:{self.buys} S:{self.sells} SB:{self.start_block} EB:{self.end_block}"

#     @property
#     def trades(self) -> int:
#         """Amount of all trades during the candle period."""
#         return self.buys + self.sells

#     @classmethod
#     def to_dataframe(cls) -> pd.DataFrame:
#         """Return emptry Pandas dataframe presenting candle data."""

#         df = pd.DataFrame(columns=Candle.DATAFRAME_FIELDS.keys())
#         return df.astype(Candle.DATAFRAME_FIELDS)

#     @classmethod
#     def to_qstrader_dataframe(cls) -> pd.DataFrame:
#         """Return emptry Pandas dataframe presenting candle data for QStrader.

#         TODO: Fix QSTrader to use "standard" column names.
#         """

#         fields = dict([
#             ("pair_id", "int"),
#             ("Date", "datetime64[s]"),
#             ("exchange_rate", "float"),
#             ("Open", "float"),
#             ("Close", "float"),
#             ("High", "float"),
#             ("Low", "float"),
#             ("buys", "float"),
#             ("sells", "float"),
#             ("volume", "float"),
#             ("buy_volume", "float"),
#             ("sell_volume", "float"),
#             ("avg", "float"),
#             ("start_block", "float"),
#             ("end_block", "float"),
#         ])
#         df = pd.DataFrame(columns=fields.keys())
#         return df.astype(fields)

#     @classmethod
#     def to_pyarrow_schema(cls, small_candles=False) -> pa.Schema:
#         """Construct schema for writing Parquet filess for these candles.

#         :param small_candles: Use even smaller word sizes for frequent (1m) candles.
#         """
#         schema = pa.schema([
#             ("pair_id", pa.uint32()),
#             ("timestamp", pa.timestamp("s")),
#             ("exchange_rate", pa.float32()),
#             ("open", pa.float32()),
#             ("close", pa.float32()),
#             ("high", pa.float32()),
#             ("low", pa.float32()),
#             ("buys", pa.uint16() if small_candles else pa.uint32()),
#             ("sells", pa.uint16() if small_candles else pa.uint32()),
#             ("volume", pa.float32()),
#             ("buy_volume", pa.float32()),
#             ("sell_volume", pa.float32()),
#             ("avg", pa.float32()),
#             ("start_block", pa.uint32()),   # Should we good for 4B blocks
#             ("end_block", pa.uint32()),
            
#         ])
#         return schema

#     @staticmethod
#     def generate_synthetic_sample(
#             pair_id: int,
#             timestamp: pd.Timestamp,
#             price: float) -> dict:
#         """Generate a candle dataframe.

#         Used in testing when manually fiddled data is needed.

#         All open/close/high/low set to the same price.
#         Exchange rate is 1.0. Other data set to zero.

#         :return:
#             One dict of filled candle data

#         """

#         return {
#             "pair_id": pair_id,
#             "timestamp": timestamp,
#             "open": price,
#             "high": price,
#             "low": price,
#             "close": price,
#             "exchange_rate": 1.0,
#             "buys": 0,
#             "sells": 0,
#             "avg": 0,
#             "start_block": 0,
#             "end_block": 0,
#             "volume": 0,
#             "buy_volume": 0,
#             "sell_volume": 0,
#         }