"""Tradind OHLCV candle data and manipulation.

See

- :py:class:`Candle` for information about :term:`OHLCV` data presentation

- :py:meth:`tradingstrategy.client.Client.fetch_candle_universe` how to load OHLCV dataset

You are likely to working with candle datasets that are presented by

- :py:class:`GroupedCandleUniverse`

For more information about candles see :term:`candle` in glossary.
"""

import datetime
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
import pyarrow as pa
from dataclasses_json import dataclass_json

from tradingstrategy.caip import ChainAddressTuple
from tradingstrategy.types import UNIXTimestamp, USDollarAmount, BlockNumber, PrimaryKey
from tradingstrategy.utils.groupeduniverse import PairGroupedUniverse



class CandleSampleUnavailable(Exception):
    """We tried to look up price for a trading pair, but count not find a candle close to the timestamp."""


@dataclass_json
@dataclass
class Candle:
    """Data structure presenting one OHLCV trading candle.

    Based on the :term:`open-high-low-close-volume <OHLCV>` concept.

    Trading Strategy candles come with additional information available on the top of core OHLCV,
    as chain analysis has deeper visibility than one would get on traditional exchanges.
    For example for enhanced attributes see :py:attr:`Candle.buys` (buy count) or
    :py:attr:`Candle.start_block` (blockchain starting block number of the candle).

    We also separate "buys" and "sells". Although this separation might not be meaningful
    on order-book based exchanges, we define "buy" as a DEX swap where quote token (USD, ETH)
    was swapped into more exotic token (AAVE, SUSHI, etc.)
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

    #: The first blockchain block that includes trades that went into this candle.
    start_block: BlockNumber

    #: The last blockchain block that includes trades that went into this candle.
    end_block: BlockNumber

    def __repr__(self):
        human_timestamp = datetime.datetime.utcfromtimestamp(self.timestamp)
        return f"@{human_timestamp} O:{self.open} H:{self.high} L:{self.low} C:{self.close} V:{self.volume} B:{self.buys} S:{self.sells} SB:{self.start_block} EB:{self.end_block}"

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
    def to_qstrader_dataframe(cls) -> pd.DataFrame:
        """Return emptry Pandas dataframe presenting candle data for QStrader.

        TODO: Fix QSTrader to use "standard" column names.
        """

        fields = dict([
            ("pair_id", "int"),
            ("Date", "datetime64[s]"),
            ("exchange_rate", "float"),
            ("Open", "float"),
            ("Close", "float"),
            ("High", "float"),
            ("Low", "float"),
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
    """Server-reply for live queried candle data.

    Uses `dataclasses-json` module for JSON serialisation.
    """

    #: A bunch of candles.
    #: Candles are unordered and subject to client side sorting.
    #: Multiple pairs and chains may be present in candles.
    candles: List[Candle]

    def sort_by_timestamp(self):
        """In-place sorting of candles by their timestamp."""
        self.candles.sort(key=lambda c: c.timestamp)


class GroupedCandleUniverse(PairGroupedUniverse):
    """A candle universe where each trading pair has its own candles.

    This is helper class to create foundation for multi pair strategies.

    For the data logistics purposes, all candles are lumped together in single columnar data blobs.
    However, it rarely makes sense to execute operations over different trading pairs.
    :py:class`GroupedCandleUniverse` creates trading pair id -> candle data grouping out from
    raw candle data.

    Usage:

    .. code-block::

        # Get candles for SUSHI-USDT

        exchange_universe = client.fetch_exchange_universe()
        raw_pairs = client.fetch_pair_universe().to_pandas()
        raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()

        pair_universe = PandasPairUniverse(raw_pairs)
        candle_universe = GroupedCandleUniverse(raw_candles)

        # Do some test calculations for a single pair
        sushi_swap = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "sushi")
        sushi_usdt = pair_universe.get_one_pair_from_pandas_universe(sushi_swap.exchange_id, "SUSHI", "USDT")

        raw_candles = client.fetch_all_candles(TimeBucket.d7).to_pandas()
        candle_universe = GroupedCandleUniverse(raw_candles)
        sushi_usdth_candles = candle_universe.get_candles_by_pair(sushi_usdt.pair_id)

    """

    def get_candle_count(self) -> int:
        """Return the dataset size - how many candles total"""
        return self.get_sample_count()

    def get_candles_by_pair(self, pair_id: PrimaryKey) -> Optional[pd.DataFrame]:
        """Get candles for a single pair.

        :return:
            Pandas dataframe object with the following columns

            - timestamp

            - open

            - high

            - low

            - close
        """
        return self.get_samples_by_pair(pair_id)

    def get_closest_price(self, pair_id: PrimaryKey, when: pd.Timestamp, kind="close", look_back_time_frames=5) -> USDollarAmount:
        """Get the available liuqidity for a trading pair at a specific timepoint or some candles before the timepoint.

        The liquidity is defined as one-sided as in :term:`XY liquidity model`.

        :param pair_id:
            Trading pair id

        :param when:
            Timestamp to query

        :param kind:
            One of OHLC data points: "open", "close", "low", "high"

        :param look_back_timeframes:
            If there is no liquidity sample available at the exact timepoint,
            look to the past to the get the nearest sample.
            For example if candle time interval is 5 minutes and look_back_timeframes is 10,
            then accept a candle that is maximum of 50 minutes before the timepoint.

        :return:
            We always return a price. In the error cases an exception is raised.

        :raise CandleSampleUnavailable:
            There was no samples available with the given condition.
        """

        assert kind in ("open", "close", "high", "low"), f"Got kind: {kind}"

        start_when = when
        samples_per_pair = self.get_candles_by_pair(pair_id)
        assert samples_per_pair is not None, f"No candle data available for pair {pair_id}"

        samples_per_kind = samples_per_pair[kind]
        for attempt in range(look_back_time_frames):
            try:
                sample = samples_per_kind[when]
                return sample
            except KeyError:
                # Go to the previous sample
                when -= self.time_bucket.to_timedelta()

        # Try to be helpful with the errors here,
        # so one does not need to open ipdb to inspect faulty data
        try:
            first_sample = samples_per_pair.iloc[0]
            second_sample = samples_per_pair.iloc[1]
            last_sample = samples_per_pair.iloc[-1]
        except KeyError:
            raise CandleSampleUnavailable(
                f"Could not find any candles for pair {pair_id}, value kind '{kind}', between {when} - {start_when}\n"
                f"Could not figure out existing data range. Has {len(samples_per_kind)} samples."
            )

        raise CandleSampleUnavailable(
            f"Could not find any candles for pair {pair_id}, value kind '{kind}', between {when} - {start_when}\n"
            f"The pair has {len(samples_per_kind)} candles between {first_sample['timestamp']} - {last_sample['timestamp']}\n"
            f"Sample interval is {second_sample['timestamp'] - first_sample['timestamp']}"
            )

    @staticmethod
    def create_empty() -> "GroupedCandleUniverse":
        """Return an empty GroupedCandleUniverse"""
        return GroupedCandleUniverse(df=Candle.to_dataframe())

    @staticmethod
    def create_empty_qstrader() -> "GroupedCandleUniverse":
        """Return an empty GroupedCandleUniverse.

        TODO: Fix QSTrader to use "standard" column names.
        """
        return GroupedCandleUniverse(df=Candle.to_qstrader_dataframe(), timestamp_column="Date")

    @staticmethod
    def create_from_single_pair_dataframe(df: pd.DataFrame) -> "GroupedCandleUniverse":
        """Construct universe based on a single trading pair data.

        Useful for synthetic data/testing.
        """
        return GroupedCandleUniverse(df)


def is_candle_green(candle: pd.Series) -> bool:
    """Check if an OHLCV candle is green.

    A :term:`OHLCV` candle is green if close is higher than open.

    Example:

    .. code-block:: python

        candle = indexed_candles.loc[pd.Timestamp("2022-02-14")]
        assert not is_candle_green(candle)
        assert is_candle_red(candle)

    """
    assert isinstance(candle, pd.Series), f"Got: {candle.__class__}"
    return candle["close"] >= candle["open"]


def is_candle_red(candle: pd.Series) -> bool:
    """Check if an OHLCV candle is green.

    A :term:`OHLCV` candle is green if close is higher than open.


    Example:

    .. code-block:: python

        candle = indexed_candles.loc[pd.Timestamp("2022-02-14")]
        assert not is_candle_green(candle)
        assert is_candle_red(candle)
    """
    assert isinstance(candle, pd.Series), f"Got: {candle.__class__}"
    return not is_candle_green(candle)
