"""Tradind OHLCV candle data and manipulation.

See

- :py:class:`Candle` for information about :term:`OHLCV` data presentation

- :py:meth:`tradingstrategy.client.Client.fetch_candle_universe` how to load OHLCV dataset

You are likely to working with candle datasets that are presented by

- :py:class:`GroupedCandleUniverse`

For more information about candles see :term:`candle` in glossary.
"""

import datetime
import warnings
from dataclasses import dataclass
from typing import List, Optional, Tuple, TypedDict, Iterable, cast

import pandas as pd
import pyarrow as pa
from dataclasses_json import dataclass_json

from tradingstrategy.chain import ChainId
from tradingstrategy.pair import DEXPair
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.types import UNIXTimestamp, USDollarAmount, BlockNumber, PrimaryKey, NonChecksummedAddress
from tradingstrategy.utils.groupeduniverse import PairGroupedUniverse
from tradingstrategy.utils.time import ZERO_TIMEDELTA

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

    #: Number of buys happened during the candle period.
    #:
    #: Only avaiable on DEXes where buys and sells can be separaed.
    buys: int | None

    #: Number of sells happened during the candle period
    #:
    #: Only avaiable on DEXes where buys and sells can be separaed.    
    sells: int | None

    #: Trade volume
    volume: USDollarAmount

    #: Buy side volume
    #:
    #: Swap quote token -> base token volume
    buy_volume: USDollarAmount | None

    #: Sell side volume
    #:
    #: Swap base token -> quote token volume
    sell_volume: USDollarAmount | None

    #: Average trade size
    avg: USDollarAmount

    #: The first blockchain block that includes trades that went into this candle.
    start_block: BlockNumber

    #: The last blockchain block that includes trades that went into this candle.
    end_block: BlockNumber

    #: TODO: Currently disabled to optimise speed
    #:
    #: This candle contained bad wicked :py:attr:`high` or :py:attr:`low` data and was filtered out.
    #:
    #: See :py:func:`tradingstrategy.utils.groupeduniverse.filter_bad_high_low`.
    #: These might be natural causes for the bad data. However,
    #: we do not want to deal with these situations inside a trading strategy.
    #: Thus, we fix candles with unrealisitc high and low wicks during the
    #: data loading.
    #:
    #: Not set unless the filter has been run on the fetched data.
    # wick_filtered: Optional[bool] = None,

    #: Schema definition for :py:class:`pd.DataFrame:
    #:
    #: Defines Pandas datatypes for columns in our candle data format.
    #: Useful e.g. when we are manipulating JSON/hand-written data.
    #:
    DATAFRAME_FIELDS = dict([
        ("pair_id", "int"),
        ("timestamp", "datetime64[s]"),
        ("exchange_rate", "float"),
        ("open", "float"),
        ("close", "float"),
        ("high", "float"),
        ("low", "float"),
        ("buys", "float"),
        ("sells", "float"),
        ("volume", "float"),
        ("buy_volume", "float"),
        ("sell_volume", "float"),
        ("avg", "float"),
        ("start_block", "int"),
        ("end_block", "int"),
    ])

    def __repr__(self):
        human_timestamp = datetime.datetime.utcfromtimestamp(self.timestamp)
        return f"@{human_timestamp} O:{self.open} H:{self.high} L:{self.low} C:{self.close} V:{self.volume} B:{self.buys} S:{self.sells} SB:{self.start_block} EB:{self.end_block}"

    @property
    def trades(self) -> int:
        """Amount of all trades during the candle period."""
        return self.buys + self.sells

    @classmethod
    def to_dataframe(cls) -> pd.DataFrame:
        """Return empty Pandas dataframe presenting candle data."""

        df = pd.DataFrame(columns=Candle.DATAFRAME_FIELDS.keys())
        return df.astype(Candle.DATAFRAME_FIELDS)

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
            ("volume", "float"),
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
            ("volume", pa.float32()),
            ("buy_volume", pa.float32()),
            ("sell_volume", pa.float32()),
            ("avg", pa.float32()),
            ("start_block", pa.uint32()),   # Should we good for 4B blocks
            ("end_block", pa.uint32()),
            
        ])
        return schema

    @staticmethod
    def generate_synthetic_sample(
            pair_id: int,
            timestamp: pd.Timestamp,
            price: float) -> dict:
        """Generate a candle dataframe.

        Used in testing when manually fiddled data is needed.

        All open/close/high/low set to the same price.
        Exchange rate is 1.0. Other data set to zero.

        :return:
            One dict of filled candle data

        """

        return {
            "pair_id": pair_id,
            "timestamp": timestamp,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "exchange_rate": 1.0,
            "buys": 0,
            "sells": 0,
            "avg": 0,
            "start_block": 0,
            "end_block": 0,
            "volume": 0,
            "buy_volume": 0,
            "sell_volume": 0,
        }



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

    def get_candles_by_pair(self, pair: "PrimaryKey | tradingstrategy.pair.DEXPair") -> Optional[pd.DataFrame]:
        """Get candles for a single pair.

        :param pair:
            Pair internal id or ``DEXPair`` instance of a trading pair.

        :return:
            Pandas dataframe object with the following columns.

            Return ``None`` if there is no candle data for this ``pair_id``.

            - timestamp

            - open

            - high

            - low

            - close
        """

        if isinstance(pair, DEXPair):
            pair_id = pair.pair_id
        else:
            pair_id = pair

        if pair_id not in self.candles_cache:
            try:
                self.candles_cache[pair_id] = self.get_samples_by_pair(pair_id)
            except KeyError:
                return None
        
        return self.candles_cache[pair_id]

    def get_closest_price(self,
                          pair: PrimaryKey | DEXPair,
                          when: pd.Timestamp | datetime.datetime,
                          kind="close",
                          look_back_time_frames=5) -> USDollarAmount:
        """Get the available price for a trading pair at a specific timepoint or some candles before the timepoint.

        .. warning ::

            This is a slow lookup method. You might want to use :py:meth:`get_price_with_tolerance`
            instead.

        :param pair:
            Trading pair id or pair object.

        :param when:
            Timestamp to query.

            This is rounded down to the nearest time bucket.
            E.g. when using 1d candles, ``2023-09-30 22:04:00.383776``
            will be rounded down to ``2023-09-30 00:00``.

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

        warnings.warn('This method is deprecated. Use GroupedCandleUniverse.get_price_with_tolerance() instead', DeprecationWarning, stacklevel=2)

        assert kind in ("open", "close", "high", "low"), f"Got kind: {kind}"

        if isinstance(when, datetime.datetime):
            when = pd.Timestamp(when)

        if isinstance(pair, DEXPair):
            pair_id = pair.pair_id
            pair_name = pair.get_ticker()
            link = pair.get_link()
        elif type(pair) == int:
            pair_id = pair
            pair_name = str(pair_id)
            link = "<link unavailable>"
        else:
            raise AssertionError(f"Unknown pair type: {pair.__class__}")

        when = when.round(self.time_bucket.to_frequency())

        start_when = when
        try:
            samples_per_pair = self.get_candles_by_pair(pair_id)
        except KeyError as e:
            raise CandleSampleUnavailable(f"Candle data missing for pair {pair_name}") from e

        assert samples_per_pair is not None, f"No candle data available for pair {pair_name}"

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
                f"Could not find any candles for pair {pair_name}, value kind '{kind}', between {when} - {start_when}\n"
                f"Could not figure out existing data range. Has {len(samples_per_kind)} samples.\n"
                f"Trading pair data can be viewed at {link}"
            )

        raise CandleSampleUnavailable(
            f"Could not find any candles for pair {pair_name}, value kind '{kind}', between {when} - {start_when}\n"
            f"The pair has {len(samples_per_kind)} candles between {first_sample['timestamp']} - {last_sample['timestamp']}\n"
            f"Sample interval is {second_sample['timestamp'] - first_sample['timestamp']}\n"
            f"Trading pair data can be viewed at {link}"
            )

    def get_price_with_tolerance(
            self,
            pair: PrimaryKey | DEXPair,
            when: pd.Timestamp | datetime.datetime,
            tolerance: pd.Timedelta,
            kind="close",
            pair_name_hint: str | None=None,
    ) -> Tuple[USDollarAmount, pd.Timedelta]:
        """Get the price for a trading pair at a specific time point, or before within a time range tolerance.

        The data may be sparse data. There might not be sample available in the same time point or
        immediate previous time point. In this case the method looks back for the previous
        data point within `tolerance` time range.

        This method should be relative fast and optimised for any price, volume and liquidity queries.

        Example:

        .. code-block:: python

            test_price, distance = universe.get_price_with_tolerance(
                pair_id=1,
                when=pd.Timestamp("2020-02-01 00:05"),
                tolerance=pd.Timedelta(30, "m"))

            # Returns closing price of the candle 2020-02-01 00:00,
            # which is 5 minutes off when we asked
            assert test_price == pytest.approx(100.50)
            assert distance == pd.Timedelta("5m")

        :param pair:
            Trading pair id

        :param when:
            Timestamp to query

        :param kind:
            One of OHLC data points: "open", "close", "low", "high"

        :param tolerance:
            If there is no liquidity sample available at the exact timepoint,
            look to the past to the get the nearest sample.
            For example if candle time interval is 5 minutes and look_back_timeframes is 10,
            then accept a candle that is maximum of 50 minutes before the timepoint.

        :param pair_name_hint:
            What should we call this pair in the error messages.

            If not given, try to figure out from the context.

        :return:
            Return (price, delay) tuple.
            We always return a price. In the error cases an exception is raised.
            The delay is the timedelta between the wanted timestamp
            and the actual timestamp of the candle.

            Candles are always timestamped by their opening.

        :raise CandleSampleUnavailable:
            There were no samples available with the given condition.

        """

        assert kind in ("open", "close", "high", "low"), f"Got kind: {kind}"

        if isinstance(when, datetime.datetime):
            when = pd.Timestamp(when)

        if isinstance(pair, DEXPair):
            pair_id = pair.pair_id
            pair_name = pair.get_ticker()
            link = pair.get_link()
        elif type(pair) == int:
            pair_id = pair
            pair_name = pair_name_hint or str(pair_id)
            link = "<link unavailable>"
        else:
            raise AssertionError(f"Unknown pair type: {pair.__class__}")

        last_allowed_timestamp = when - tolerance

        candles_per_pair = self.get_candles_by_pair(pair_id)

        if candles_per_pair is None:
            uniq_pairs = self.get_pair_count()
            raise CandleSampleUnavailable(
                f"No candle data available for pair {pair_name}, pair id {pair_id}\n"
                f"Trading data pair link: {link}"
                f"Did you load price data for this trading pair?\n"
                f"We have price feed data loaded for {uniq_pairs} trading pairs\n"
            )

        samples_per_kind = candles_per_pair[kind]

        # Fast path
        try:
            sample = samples_per_kind[when]
            return sample, pd.Timedelta(seconds=0)
        except KeyError:
            pass

        #
        # No direct hit. Either sparse data or data not available before this.
        # Lookup just got complicated,
        # like our relationship on Facebook.
        #

        # The indexes we can have are
        # - MultiIndex (pair id, timestamp) - if multiple trading pairs present
        # - DatetimeIndex - if single trading pair present

        if isinstance(candles_per_pair.index, pd.MultiIndex):
            timestamp_index = cast(pd.DatetimeIndex, candles_per_pair.index.get_level_values(1))
        elif isinstance(candles_per_pair.index, pd.DatetimeIndex):
            timestamp_index = candles_per_pair.index
        else:
            raise NotImplementedError(f"Does not know how to handle index {candles_per_pair.index}")

        # TODO: Do we need to cache the indexer... does it has its own storage?
        ffill_indexer = timestamp_index.get_indexer([when], method="ffill")

        before_match_iloc = ffill_indexer[0]

        if before_match_iloc < 0:
            # We get -1 if there are no timestamps where the forward fill could start
            first_sample_timestamp = timestamp_index[0]
            raise CandleSampleUnavailable(
                f"Could not find any candles for pair {pair_name}, value kind '{kind}' at or before {when}\n"
                f"- Pair has {len(samples_per_kind)} samples\n"
                f"- First sample is at {first_sample_timestamp}\n"
                f"- Trading pair page link {link}\n"
            )
        before_match = timestamp_index[before_match_iloc]

        latest_or_equal_sample = candles_per_pair.iloc[before_match_iloc]

        # Check if the last sample before the cut off is within time range our tolerance
        candle_timestamp = before_match

        # Internal sanity check
        distance = when - candle_timestamp
        assert distance >= ZERO_TIMEDELTA, f"Somehow we managed to get a candle timestamp {candle_timestamp} that is newer than asked {when}"

        if candle_timestamp >= last_allowed_timestamp:
            # Return the chosen price column of the sample,
            # because we are within the tolerance
            return latest_or_equal_sample[kind], distance

        # We have data, but we are out of tolerance
        first_sample_timestamp = timestamp_index[0]
        last_sample_timestamp = timestamp_index[-1]

        raise CandleSampleUnavailable(
            f"Could not find candle data for pair {pair_name}\n"
            f"- Column '{kind}'\n"
            f"- At {when}\n"
            f"- Lower bound of time range tolerance {last_allowed_timestamp}\n"
            f"\n"
            f"- Data lag tolerance is set to {tolerance}\n"
            f"- The pair has {len(samples_per_kind)} candles between {first_sample_timestamp} - {last_sample_timestamp}\n"
            f"\n"
            f"Data unavailability might be due to several reasons:\n"
            f"\n"
            f"- You are handling sparse data - trades have not been made or the blockchain was halted during the price look-up period.\n"
            f"  Try to increase 'tolerance' argument time window.\n"
            f"- You are asking historical data when the trading pair was not yet live.\n"
            f"- Your backtest is using indicators that need more lookback buffer than you are giving to them.\n"
            f"  Try set your data load range earlier or your backtesting starting later.\n"
            f"\n"
            f"Trading pair page link: {link}"
            )

    @staticmethod
    def create_empty() -> "GroupedCandleUniverse":
        """Return an empty GroupedCandleUniverse"""
        return GroupedCandleUniverse(df=Candle.to_dataframe(), fix_wick_threshold=None)

    @staticmethod
    def create_empty_qstrader() -> "GroupedCandleUniverse":
        """Return an empty GroupedCandleUniverse.

        TODO: Fix QSTrader to use "standard" column names.
        """
        return GroupedCandleUniverse(df=Candle.to_qstrader_dataframe(), timestamp_column="Date", fix_wick_threshold=None)

    @staticmethod
    def create_from_single_pair_dataframe(
            df: pd.DataFrame,
            bucket: TimeBucket | None = None,
    ) -> "GroupedCandleUniverse":
        """Construct universe based on a single trading pair data.

        Useful for synthetic data/testing.
        """
        return GroupedCandleUniverse(df, time_bucket=bucket)

    @staticmethod
    def create_from_multiple_candle_datafarames(dfs: Iterable[pd.DataFrame]) -> "GroupedCandleUniverse":
        """Construct universe based on multiple trading pairs.

        Useful for synthetic data/testing.

        :param dfs:
            List of dataframes/series where each trading pair is as isolated
            OHLCV data feed.
        """
        merged = pd.concat(dfs)
        return GroupedCandleUniverse(merged)


class TradingPairDataAvailability(TypedDict):
    """Trading data availability description for a single pair.

    - Trading Strategy oracle uses sparse data format where candles
      with zero trades are not generated. This is better suited
      for illiquid DEX markets with few trades.

    - Because of sparse data format, we do not know if there is a last
      candle available - candle may not be available yet or there might not be trades
      to generate a candle

    - This information is always time frame (15m, 1h, 1d) specific

    - See :py:meth:`tradingstrategy.client.Client.fetch_trading_data_availability`
    """

    #: Blockchain of the pair
    chain_id: ChainId

    #: Address of the pair
    pair_address: NonChecksummedAddress

    #: Internal id of the pair
    pair_id: PrimaryKey

    #: What is the last trade oracle has seen for this trading pair.
    #:
    #: This trade might not be rolled up to a candle yet.
    last_trade_at: datetime.datetime

    #: What is the last full available candle for this trading pair
    last_candle_at: datetime.datetime

    #: What might be the last available candle for this trading pair
    #: 
    #: In Uniswap v3, there might not be a candle available due to low liquidity,
    #: in that case we can use this timestamp to mark the data is fine up to this point.
    last_supposed_candle_at: datetime.datetime


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
