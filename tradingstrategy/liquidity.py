"""Liquidity data feed manipulation.

For more information about liquidity in automatic market making pools see :term:`AMM`
and :term:`XY liquidity model`.
"""

import datetime
from dataclasses import dataclass
from typing import List, Optional, Iterable, Tuple

import pandas as pd
import pyarrow as pa
from dataclasses_json import dataclass_json
from pandas.core.groupby import GroupBy

from tradingstrategy.types import UNIXTimestamp, USDollarAmount, BlockNumber, PrimaryKey
from tradingstrategy.utils.groupeduniverse import PairGroupedUniverse


class LiquidityDataUnavailable(Exception):
    """We tried to look up liquidity info for a trading pair, but count not find a sample."""


@dataclass_json
@dataclass
class XYLiquidity:
    """Data structure that presents liquidity status in bonding curve pool.

    This data structure is for naive x*y=k :term:`AMM` pool.
    Liquidity is not the part of the normal :term:`technical analysis`,
    so the dataset server has separate datasets for it.

    Liquidity is expressed as US dollar value of the :term:`quote token` of the pool.
    For example if the pool is 50 $FOO in reserve0 and 50 $USDC in reserve1, the
    liquidity of the pool would be expressed as 50 USD.

    Liquidity events, like :term:`candles <candle>`, have open, high, low and close values,
    depending on which time of the candle they were sampled.
    """

    #: Primary key to identity the trading pair
    #: Use pair universe to map this to chain id and a smart contract address
    pair_id: PrimaryKey

    #: Open timestamp for this time bucket.
    timestamp: UNIXTimestamp

    #: USD exchange rate of the quote token used to
    #: convert to dollar amounts in this time bucket.
    #:
    #: Note that currently any USD stablecoin (USDC, DAI) is
    #: assumed to be 1:1 and the candle server cannot
    #: handle exchange rate difference among stablecoins.
    #:
    #: The rate is taken at the beginning of the 1 minute time bucket.
    #: For other time buckets, the exchange rate is the simple average
    #: for the duration of the bucket.
    exchange_rate: float

    #: Liquidity absolute values in the pool in different time points.
    #: Note - for minute candles - if the candle contains only one event (mint, burn, sync)
    #: the open liquidity value is the value AFTER this event.
    #: The dataset server does not track the closing value of the previous liquidity event.
    #: This applies for minute candles only.
    open: USDollarAmount

    #: Liquidity absolute values in the pool in different time points
    close: USDollarAmount

    #: Liquidity absolute values in the pool in different time points
    high: USDollarAmount

    #: Liquidity absolute values in the pool in different time points
    low: USDollarAmount

    #: Number of liquidity supplied events for pool
    adds: int

    #: Number of liquidity removed events for the pool
    removes: int

    #: Number of total events affecting liquidity during the time window.
    #: This is adds, removes AND swaps AND sync().
    syncs: int

    #: How much new liquidity was supplied, in the terms of the quote token converted to US dollar
    add_volume: USDollarAmount

    #: How much new liquidity was removed, in the terms of the quote token converted to US dollar
    add_volume: USDollarAmount

    #: Blockchain tracking information
    start_block: BlockNumber

    #: Blockchain tracking information
    end_block: BlockNumber

    def __repr__(self):
        human_timestamp = datetime.datetime.utcfromtimestamp(self.timestamp)
        return f"@{human_timestamp} O:{self.open} H:{self.high} L:{self.low} C:{self.close} V:{self.volume} A:{self.adds} R:{self.removes} SB:{self.start_block} EB:{self.end_block}"

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
            ("adds", pa.uint16() if small_candles else pa.uint32()),
            ("removes", pa.uint16() if small_candles else pa.uint32()),
            ("syncs", pa.uint16() if small_candles else pa.uint32()),
            ("add_volume", pa.float32()),
            ("remove_volume", pa.float32()),
            ("start_block", pa.uint32()),   # Should we good for 4B blocks
            ("end_block", pa.uint32()),
        ])
        return schema

    @classmethod
    def to_dataframe(cls) -> pd.DataFrame:
        """Return emptry Pandas dataframe presenting liquidity sample."""

        # TODO: Does not match the spec 1:1 - but only used as empty in tests
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
            ("add_volume", "float"),
            ("remove_volume", "float"),
            ("start_block", "float"),
            ("end_block", "float"),
        ])
        df = pd.DataFrame(columns=fields.keys())
        return df.astype(fields)


@dataclass_json
@dataclass
class LiquidityResult:
    """Server-reply for live queried liquidity data."""

    #: A bunch of candles.
    #: Candles are unordered and subject to client side sorting.
    #: Multiple pairs and chains may be present in candles.
    liquidity_events: List[XYLiquidity]

    def sort_by_timestamp(self):
        """In-place sorting of candles by their timestamp."""
        self.candles.sort(key=lambda c: c.timestamp)


class GroupedLiquidityUniverse(PairGroupedUniverse):
    """A universe where each trading pair has its own liquidity data feed.

    This is helper class to create foundation for multi pair strategies.

    For the data logistics purposes, all candles are lumped together in single columnar data blobs.
    However, it rarely makes sense to execute operations over different trading pairs.
    :py:class`GroupedLiquidityUniverse` creates trading pair id -> liquidity sample data grouping out from
    raw liquidity sample.
    """

    def get_liquidity_samples_by_pair(self, pair_id: PrimaryKey) -> Optional[pd.DataFrame]:
        """Get samples for a single pair.

        If the pair does not exist return `None`.
        """
        try:
            return self.get_samples_by_pair(pair_id)
        except KeyError:
            return None

    def get_closest_liquidity(self, pair_id: PrimaryKey, when: pd.Timestamp, kind="open", look_back_time_frames=5) -> USDollarAmount:
        """Get the available liuqidity for a trading pair at a specific timepoint or some candles before the timepoint.

        The liquidity is defined as one-sided as in :term:`XY liquidity model`.

        :param pair_id: Traing pair id
        :param when: Timestamp to query
        :param kind: One of liquidity samples: "open", "close", "low", "high"
        :param look_back_timeframes: If there is no liquidity sample available at the exact timepoint,
            look to the past to the get the nearest sample
        :return: We always return
        :raise LiquidityDataUnavailable: There was no liquidity sample available
        """

        assert kind in ("open", "close", "high", "low"), f"Got kind: {kind}"

        start_when = when
        samples_per_pair = self.get_liquidity_samples_by_pair(pair_id)
        assert samples_per_pair is not None, f"No liquidity data available for pair {pair_id}"

        samples_per_kind = samples_per_pair[kind]
        for attempt in range(look_back_time_frames):
            try:
                sample = samples_per_kind[when]
                return sample
            except KeyError:
                # Go to the previous sample
                when -= self.time_bucket.to_timedelta()

        raise LiquidityDataUnavailable(f"Could not find any liquidity samples for pair {pair_id} between {when} - {start_when}")

    @staticmethod
    def create_empty() -> "GroupedLiquidityUniverse":
        """Create a liquidity universe without any data."""
        return GroupedLiquidityUniverse(df=XYLiquidity.to_dataframe(), index_automatically=False)
