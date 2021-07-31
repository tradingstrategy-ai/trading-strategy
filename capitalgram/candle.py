"""Tradind data candle data and manipulation.

For more information about candles see :term:`candle`.
"""

import datetime
from dataclasses import dataclass
from typing import List, Optional, Iterable, Tuple

import pandas as pd
import pyarrow as pa
from dataclasses_json import dataclass_json
from pandas.core.groupby import GroupBy

from capitalgram.caip import ChainAddressTuple
from capitalgram.types import UNIXTimestamp, USDollarAmount, BlockNumber, PrimaryKey


@dataclass_json
@dataclass
class Candle:
    """Data structure that presents one candle in Capitalgram.

    Based on the :term:`open-high-low-close-volume <OHLCV>` concept.

    Capitalgram candles come with additional information available on the top of core OHLCV.
    This is because our chain analysis has deeper visibility than one would
    get on traditional exchanges.
    """

    #: Primary key to identity the trading pair
    #: Use pair universe to map this to chain id and a smart contract address
    pair_id: PrimaryKey

    #: Open timestamp for this candle.
    #: Note that the close timestamp you need to supply yourself based on the context.
    timestamp: UNIXTimestamp  # UNIX timestamp as seconds

    #: USD exchange rate of the quote token used to
    #: convert to dollar amounts in this candle.
    #:
    #: Note that currently any USD stablecoin (USDC, DAI) is
    #: assumed to be 1:1 and the candle server cannot
    #: handle exchange rate difference among stablecoins.
    #:
    #: The rate is taken at the beginning of the 1 minute time bucket.
    #: For other time buckets, the exchange rate is the simple average
    #: for the duration of the bucket.
    exchange_rate: float

    #: OHLC core data
    open: USDollarAmount

    #: OHLC core data
    close: USDollarAmount

    #: OHLC core data
    high: USDollarAmount

    #: OHLC core data
    low: USDollarAmount

    #: Number of buys happened during the candle period
    buys: int

    #: Number of sells happened during the candle period
    sells: int

    #: Volume data
    buy_volume: USDollarAmount

    #: Volume data
    sell_volume: USDollarAmount

    #: Average trade size
    avg: USDollarAmount

    #: Blockchain tracking information
    start_block: BlockNumber

    #: Blockchain tracking information
    end_block: BlockNumber

    def __repr__(self):
        human_timestamp = datetime.datetime.utcfromtimestamp(self.timestamp)
        return f"@{human_timestamp} O:{self.open} H:{self.high} L:{self.low} C:{self.close} V:{self.volume} B:{self.buys} S:{self.sells} SB:{self.start_block} EB:{self.end_block}"

    @property
    def caip(self) -> ChainAddressTuple:
        """Unique identifier for the trading pair"""
        return ChainAddressTuple(self.chain_id.value, self.address)

    @property
    def trades(self) -> int:
        """Amount of all trades during the candle period."""
        return self.buys + self.sells

    @property
    def volume(self) -> USDollarAmount:
        """Total volume during the candle period.

        Unline in traditional CEX trading, we can separate buy volume and sell volume from each other,
        becauase liquidity provider is a special role.
        """
        return self.buy_volume + self.sell_volume

    @classmethod
    def to_dataframe(cls) -> pd.DataFrame:
        """Return emptry Pandas dataframe presenting candle data."""

        fields = dict([
            ("pair_id", "int"),
            ("timestamp", "datetime64[s]"),
            ("exchange_rate", "float"),
            ("open", "float"),
            ("close", "float"),
            ("high", "float"),
            ("low", "float"),
            ("buys", "float"),
            ("sells", "float"),
            ("buy_volume", "float"),
            ("sell_volume", "float"),
            ("avg", "float"),
            ("start_block", "float"),
            ("end_block", "float"),
        ])
        df = pd.DataFrame(columns=fields.keys())
        return df.astype(fields)


    @classmethod
    def to_pyarrow_schema(cls, small_candles=False) -> pa.Schema:
        """Construct schema for writing Parquet filess for these candles.

        :param small_candles: Use even smaller word sizes for frequent (1m) candles.
        """
        schema = pa.schema([
            ("pair_id", pa.uint32()),
            ("timestamp", pa.timestamp("s")),
            ("exchange_rate", pa.float32()),
            ("open", pa.float32()),
            ("close", pa.float32()),
            ("high", pa.float32()),
            ("low", pa.float32()),
            ("buys", pa.uint16() if small_candles else pa.uint32()),
            ("sells", pa.uint16() if small_candles else pa.uint32()),
            ("buy_volume", pa.float32()),
            ("sell_volume", pa.float32()),
            ("avg", pa.float32()),
            ("start_block", pa.uint32()),   # Should we good for 4B blocks
            ("end_block", pa.uint32()),
        ])
        return schema



@dataclass_json
@dataclass
class CandleResult:
    """Server-reply for live queried candle data."""

    #: A bunch of candles.
    #: Candles are unordered and subject to client side sorting.
    #: Multiple pairs and chains may be present in candles.
    candles: List[Candle]

    def sort_by_timestamp(self):
        """In-place sorting of candles by their timestamp."""
        self.candles.sort(key=lambda c: c.timestamp)


class GroupedCandleUniverse:
    """A candle universe where each trading pair has its own candles.

    This is helper class to create foundation for multi pair strategies.

    For the data logistics purposes, all candles are lumped together in single columnar data blobs.
    However, it rarely makes sense to execute operations over different trading pairs.
    :py:class`GroupedCandleUniverse` creates trading pair id -> candle data grouping out from
    raw candle data.
    """

    def __init__(self, df: pd.DataFrame):
        assert isinstance(df, pd.DataFrame)
        self.df = df
        self.pairs: GroupBy = df.groupby(["pair_id"])

    def get_candles_by_pair(self, pair_id: PrimaryKey) -> Optional[pd.DataFrame]:
        """Get candles for a single pair."""
        return self.pairs.get_group(pair_id)

    def get_all_pairs(self) -> Iterable[Tuple[PrimaryKey, pd.DataFrame]]:
        """Go through all candles, one DataFrame per trading pair."""
        for pair_id, data in self.pairs:
            yield pair_id, data

