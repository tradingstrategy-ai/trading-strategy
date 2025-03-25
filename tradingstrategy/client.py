"""Trading strategy client.

A Python client class to downlaod different datasets from `Trading Strategy oracle <https://tradingstrategy.ai>`_.

For usage see

- :py:class:`Client` class

"""
import datetime
import logging
import os
import tempfile
import time
import warnings
from abc import abstractmethod, ABC
from functools import wraps
from json import JSONDecodeError
from pathlib import Path
from typing import Final, Optional, Union, Collection, Dict, Literal

import pandas as pd

from tradingstrategy.candle import TradingPairDataAvailability
from tradingstrategy.environment.default_environment import DefaultClientEnvironment, DEFAULT_SETTINGS_PATH
from tradingstrategy.reader import BrokenData, read_parquet
from tradingstrategy.token_metadata import TokenMetadata
from tradingstrategy.top import TopPairsReply, TopPairMethod
from tradingstrategy.transport.pyodide import PYODIDE_API_KEY
from tradingstrategy.types import PrimaryKey, AnyTimestamp, USDollarAmount
from tradingstrategy.lending import LendingReserveUniverse, LendingCandleType, LendingCandleResult

# TODO: Must be here because  warnings are very inconveniently triggered import time
from tqdm import TqdmExperimentalWarning
warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)
from tqdm_loggable.auto import tqdm


with warnings.catch_warnings():
    # Work around this warning
    # ../../../Library/Caches/pypoetry/virtualenvs/tradeexecutor-Fzci9y7u-py3.9/lib/python3.9/site-packages/marshmallow/__init__.py:17
    #   /Users/mikkoohtamaa/Library/Caches/pypoetry/virtualenvs/tradeexecutor-Fzci9y7u-py3.9/lib/python3.9/site-packages/marshmallow/__init__.py:17: DeprecationWarning: distutils Version classes are deprecated. Use packaging.version instead.
    #     __version_info__ = tuple(LooseVersion(__version__).version)
    warnings.simplefilter("ignore")
    import dataclasses_json  # Trigger marsmallow import to supress the warning

import pyarrow
import pyarrow as pa
from pyarrow import Table

from tradingstrategy.chain import ChainId
from tradingstrategy.environment.base import Environment, download_with_progress_plain
from tradingstrategy.environment.config import Configuration

from tradingstrategy.exchange import ExchangeUniverse
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.transport.cache import CachedHTTPTransport, DataNotAvailable, OHLCVCandleType

logger = logging.getLogger(__name__)


RETRY_DELAY: Final[int] = 30  # seconds

MAX_ATTEMPTS: Final[int] = 3

#: Default HTTP timeout conn/read
DEFAULT_TIMEOUT = (89.0, 89.0)

def _retry_corrupted_parquet_fetch(method):
    """A helper decorator to down with download/Parquet corruption issues.

    Attempt download and read 3 times. If download is corrpted, clear caches.
    """
    # https://stackoverflow.com/a/36944992/315168
    @wraps(method)
    def impl(self, *method_args, **method_kwargs):
        attempts = MAX_ATTEMPTS
        while attempts > 0:
            try:
                return method(self, *method_args, **method_kwargs)
            # TODO: Build expection list over the time by
            # observing issues in production
            except (OSError, BrokenData) as e:
                # This happens when we download Parquet file, but it is missing half
                # e.g. due to interrupted download
                attempts -= 1
                path_to_remove = e.path if isinstance(e, BrokenData) else None

                if attempts > 0:
                    logger.error("Damaged Parquet file fetch detected for method %s, attempting to re-fetch. Error was: %s", method, e)
                    logger.exception(e)

                    self.clear_caches(filename=path_to_remove)

                    logger.info(
                        f"Next parquet download retry in {RETRY_DELAY} seconds, "
                        f"{attempts}/{MAX_ATTEMPTS} attempt(s) left"
                    )
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error(
                        f"Exhausted all {MAX_ATTEMPTS} attempts, fetching parquet data failed."
                    )
                    self.clear_caches(filename=path_to_remove)
                    raise

        raise AssertionError(f"Should not be reached. Download issue on {self}, {attempts} / {MAX_ATTEMPTS}, {method_args}, {method_kwargs}")

    return impl

class BaseClient(ABC):
    """Base class for all real and test mocks clients."""

    # TODO: Move to its own module, add rest of the methods

    @abstractmethod
    def clear_caches(self, fname: str | None):
        pass


class Client(BaseClient):
    """An API client for querying the Trading Strategy datasets from a server.

    - The client will download datasets.

    - In-built disk cache is offered, so that large datasets are not redownloaded
      unnecessarily.

    - There is protection against network errors: dataset downloads are retries in the case of
      data corruption errors.

    - Nice download progress bar will be displayed (when possible)

    You can :py:class:`Client` either in

    - Jupyter Notebook environments - see :ref:`tutorial` for an example

    - Python application environments, see an example below

    - Integration tests - see :py:meth:`Client.create_test_client`

    Python application usage:

    .. code-block:: python

        import os

        trading_strategy_api_key = os.environ["TRADING_STRATEGY_API_KEY"]
        client = Client.create_live_client(api_key)
        exchanges = client.fetch_exchange_universe()
        print(f"Dataset contains {len(exchange_universe.exchanges)} exchanges")

    """

    def __init__(self, env: Environment, transport: CachedHTTPTransport):
        """Do not call constructor directly, but use one of create methods. """
        self.env = env
        self.transport = transport

    def close(self):
        """Close the streams of underlying transport."""
        self.transport.close()

    def clear_caches(self, filename: Optional[Union[str, Path]] = None):
        """Remove any cached data.

        Cache is specific to the current transport.

        :param filename:
            If given, remove only that specific file, otherwise clear all cached data.
        """
        self.transport.purge_cache(filename)

    @_retry_corrupted_parquet_fetch
    def fetch_pair_universe(self) -> pa.Table:
        """Fetch pair universe from local cache or the candle server.

        The compressed file size is around 5 megabytes.

        If the download seems to be corrupted, it will be attempted 3 times.
        """
        assert self.transport.api_key, "fetch_pair_universe(): Client.api_key missing"
        path = self.transport.fetch_pair_universe()
        assert path.exists(), f"Does not exist: {path}"
        size = path.stat().st_size
        assert size > 0, f"Parquest file size is zero {path}"
        # logger.info("Fetched pair universe to %s, file size is %d bytes", path, size)

        if size < 1024:
            # Broken file, hack to display info to user what goes wrong
            with open(path, "rt") as f:
                data = f.read()
                logger.error("Parquet %s corrupted, size %d, content %s", path, size, data)

        return read_parquet(path)

    def fetch_exchange_universe(self) -> ExchangeUniverse:
        """Fetch list of all exchanges form the :term:`dataset server`.
        """
        path = self.transport.fetch_exchange_universe()
        with path.open("rt", encoding="utf-8") as inp:
            data = inp.read()
            try:
                return ExchangeUniverse.from_json(data)
            except JSONDecodeError as e:
                raise RuntimeError(f"Could not read ExchangeUniverse JSON file {path}\nData is {data}") from e

    @_retry_corrupted_parquet_fetch
    def fetch_all_candles(self, bucket: TimeBucket) -> pyarrow.Table:
        """Get cached blob of candle data of a certain candle width.

        The returned data can be between several hundreds of megabytes to several gigabytes
        and is cached locally.

        The returned data is saved in PyArrow Parquet format.

        For more information see :py:class:`tradingstrategy.candle.Candle`.

        If the download seems to be corrupted, it will be attempted 3 times.
        """
        path = self.transport.fetch_candles_all_time(bucket)
        assert path is not None, "fetch_candles_all_time() returned None"
        return read_parquet(path)

    def fetch_candles_by_pair_ids(self,
          pair_ids: Collection[PrimaryKey],
          bucket: TimeBucket,
          start_time: Optional[datetime.datetime | pd.Timestamp] = None,
          end_time: Optional[datetime.datetime | pd.Timestamp] = None,
          max_bytes: Optional[int] = None,
          progress_bar_description: Optional[str] = None,
        ) -> pd.DataFrame:
        """Fetch candles for particular trading pairs.

        This is right API to use if you want data only for a single
        or few trading pairs. If the number
        of trading pair is small, this download is much more lightweight
        than Parquet dataset download.

        The fetch is performed using JSONL API endpoint. This endpoint
        always returns real-time information.

        :param pair_ids:
            Trading pairs internal ids we query data for.
            Get internal ids from pair dataset.

        :param time_bucket:
            Candle time frame

        :param start_time:
            All candles after this.
            If not given start from genesis.

        :param end_time:
            All candles before this

        :param max_bytes:
            Limit the streaming response size

        :param progress_bar_description:
            Display on download progress bar.

        :return:
            Candles dataframe

        :raise tradingstrategy.transport.jsonl.JSONLMaxResponseSizeExceeded:
                If the max_bytes limit is breached
        """

        if isinstance(start_time, pd.Timestamp):
            start_time = start_time.to_pydatetime()

        if isinstance(end_time, pd.Timestamp):
            end_time = end_time.to_pydatetime()

        assert len(pair_ids) > 0

        return self.transport.fetch_candles_by_pair_ids(
            pair_ids,
            bucket,
            start_time,
            end_time,
            max_bytes=max_bytes,
            progress_bar_description=progress_bar_description,
        )

    def fetch_tvl_by_pair_ids(self,
        pair_ids: Collection[PrimaryKey],
        bucket: TimeBucket,
        start_time: Optional[AnyTimestamp] = None,
        end_time: Optional[AnyTimestamp] = None,
        progress_bar_description: Optional[str] = None,
        query_type: OHLCVCandleType = OHLCVCandleType.tvl_v1,
    ) -> pd.DataFrame:
        """Fetch TVL/liquidity candles for particular trading pairs.

        This is right API to use if you want data only for a single
        or few trading pairs. If the number
        of trading pair is small, this download is much more lightweight
        than Parquet dataset download.

        TODO: Upgrade default `query_type` from `tvl_v1` to `tvl_v2`. Lots of backwards breaking changes.

        The returned TVL/liquidity data is converted to US dollars by the server.

        .. note ::

            TVL data is an estimation. Malicious tokens are known to manipulate
            their TVL/liquidity/market depth, and it is not possible
            to detect and eliminate all manipulations.

        Example:

        .. code-block:: python

            exchange_universe = client.fetch_exchange_universe()
            pairs_df = client.fetch_pair_universe().to_pandas()

            pair_universe = PandasPairUniverse(
                pairs_df,
                exchange_universe=exchange_universe,
            )

            pair = pair_universe.get_pair_by_human_description(
                (ChainId.ethereum, "uniswap-v3", "WETH", "USDC", 0.0005)
            )

            pair_2 = pair_universe.get_pair_by_human_description(
                (ChainId.ethereum, "uniswap-v2", "WETH", "USDC")
            )

            start = datetime.datetime(2024, 1, 1)
            end = datetime.datetime(2024, 2, 1)

            liquidity_df = client.fetch_tvl_by_pair_ids(
                [pair.pair_id, pair_2.pair_id],
                TimeBucket.d1,
                start_time=start,
                end_time=end,
            )

        :param pair_ids:
            Trading pairs internal ids we query data for.
            Get internal ids from pair dataset.

        :param bucket:
            Candle time frame.

            Ask `TimeBucker.d1` or higher. TVL data may not be indexed for
            for lower timeframes.

        :param start_time:
            All candles after this.
            If not given start from genesis.

        :param end_time:
            All candles before this

        :param progress_bar_description:
            Display a download progress bar using `tqdm_loggable` if given.

        :return:
            TVL dataframe.

            Has columns "open", "high", "low", "close", "pair_id" presenting
            TVL at the different points of time. The index is `DateTimeIndex`.

            This data is not forward filled.

        """

        assert bucket >= TimeBucket.d1, f"It does not make sense to fetch TVL/liquidity data with higher frequency than a day,got {bucket}"

        if isinstance(start_time, pd.Timestamp):
            start_time = start_time.to_pydatetime()

        if isinstance(end_time, pd.Timestamp):
            end_time = end_time.to_pydatetime()

        assert len(pair_ids) > 0

        return self.transport.fetch_tvl_by_pair_ids(
            pair_ids,
            bucket,
            start_time,
            end_time,
            progress_bar_description=progress_bar_description,
            query_type=query_type,
        )

    def fetch_clmm_liquidity_provision_candles_by_pair_ids(self,
        pair_ids: Collection[PrimaryKey],
        bucket: TimeBucket,
        start_time: Optional[AnyTimestamp] = None,
        end_time: Optional[AnyTimestamp] = None,
        progress_bar_description: Optional[str] = "Downloading CLMM data",
    ) -> pd.DataFrame:
        """Fetch CLMM liquidity provision candles.

        Get Uniswap v3 liquidity provision data for liquidity provider position backtesting.

        - Designed to be used with `Demeter backtesting framework <https://github.com/zelos-alpha/demeter/tree/master/demeter>`__ but works with others.

        - For the candles format see :py:mod:`tradingstrategy.clmm`.

        - Responses are cached on the local file system

        Example:

        .. code-block:: python

            import datetime
            from tradingstrategy.pair import PandasPairUniverse
            from tradingstrategy.timebucket import TimeBucket
            from tradingstrategy.chain import ChainId


            class DemeterParameters:
                pair_descriptions = [
                    (ChainId.arbitrum, "uniswap-v3", "WETH", "USDC", 0.0005)
                ]
                start = datetime.datetime(2024, 1, 1)
                end = datetime.datetime(2024, 2, 1)
                time_bucket = TimeBucket.m1
                initial_cash = 10_000  # USDC
                initial_base_token = 1  # WETH

            # Load data needed to resolve pair human descriptions to internal ids
            exchange_universe = client.fetch_exchange_universe()
            pairs_df = client.fetch_pair_universe().to_pandas()
            pair_universe = PandasPairUniverse(
                pairs_df,
                exchange_universe=exchange_universe,
            )

            # Load metadata for the chosen trading pairs (pools)
            pair_metadata = [pair_universe.get_pair_by_human_description(desc) for desc in DemeterParameters.pair_descriptions]

            # Map to internal pair primary keys
            pair_ids = [pm.pair_id for pm in pair_metadata]

            print("Pool addresses are", [(pm.get_ticker(), pm.pair_id, pm.address) for pm in pair_metadata])

            # Load CLMM data for selected pairs
            clmm_df = client.fetch_clmm_liquidity_provision_candles_by_pair_ids(
                pair_ids,
                DemeterParameters.time_bucket,
                start_time=DemeterParameters.start,
                end_time=DemeterParameters.end,
            )

            print("CLMM data sample is")
            display(clmm_df.head(10))

        :param pair_ids:
            Trading pairs internal ids we query data for.
            Get internal ids from pair dataset.

            Only works with Uniswap v3 pairs.

        :param bucket:
            Candle time frame.

            Ask `TimeBucket.d1` or lower. `TimeBucket.m1` is most useful for LP backtesting.

        :param start_time:
            All candles after this.

            Inclusive.

        :param end_time:
            All candles before this.

            Inclusive.

        :param progress_bar_description:
            Display a download progress bar using `tqdm_loggable` if given.

            Set to `None` to disable.

        :return:
            CLMM dataframe.

            See :py:mod:`tradingstrategy.clmm` for details.
        """

        assert bucket <= TimeBucket.d1, f"It does not make sense to fetch CLMM data with higher frequency than a 1 day, got {bucket}"
        assert len(pair_ids) > 0

        if isinstance(start_time, pd.Timestamp):
            start_time = start_time.to_pydatetime()

        if isinstance(end_time, pd.Timestamp):
            end_time = end_time.to_pydatetime()

        return self.transport.fetch_clmm_liquidity_provision_candles_by_pair_ids(
            pair_ids,
            bucket,
            start_time,
            end_time,
            progress_bar_description=progress_bar_description,
        )

    def fetch_tvl(self,
        bucket: TimeBucket,
        mode: Literal["min_tvl", "min_tvl_low", "pair_ids"],
        exchange_ids: Collection[PrimaryKey] = None,
        pair_ids: Collection[PrimaryKey] = None,
        start_time: Optional[AnyTimestamp] = None,
        end_time: Optional[AnyTimestamp] = None,
        min_tvl: Optional[USDollarAmount] = None,
        progress_bar_description: Optional[str] = "...",
    ) -> pd.DataFrame:
        """Fetch TVL data.

        - Get TVL data for given trading pairs

        ... or ...

        - Filter out exchange trading pairs by minimum TVL amount

        .. note ::

            If you ask too large number of pairs, or have ``min_tvl`` condition set too low,
            the endpoint will timeout because it can only serve limited amount of information.
            At this kind of cases use :py:meth:`fetch_all_liquidity_samples` static Parquet
            file download and filter down pairs yourself.

        .. note:::

            When ``min_tvl`` is used, the date range ``start`` - ``end`` is approximation. Entries outside this range
            may be returned.

        Example how to create a trading univers that picks all Uniswap pairs with ``min_tvl`` during the period:

        .. code-block:: python

            class Parameters:
                exchanges = {"uniswap-v2", "uniswap-v3"}
                min_tvl = 1_500_000
                backtest_start = datetime.datetime(2024, 1, 1)
                backtest_end = datetime.datetime(2024, 2, 4)

            SUPPORTING_PAIRS = [
                (ChainId.base, "uniswap-v2", "WETH", "USDC", 0.0030),
                (ChainId.base, "uniswap-v3", "WETH", "USDC", 0.0005),
                (ChainId.base, "uniswap-v3", "cbBTC", "WETH", 0.0030),    # Only trading since October
            ]

            exchange_universe = client.fetch_exchange_universe()
            targeted_exchanges = [exchange_universe.get_by_chain_and_slug(ChainId.base, slug) for slug in Parameters.exchanges]

            all_pairs_df = client.fetch_pair_universe().to_pandas()
            all_pairs_df = filter_for_exchange_slugs(all_pairs_df, Parameters.exchanges)
            pair_universe = PandasPairUniverse(
                all_pairs_df,
                exchange_universe=exchange_universe,
                build_index=False,
            )

            #
            # Do exchange and TVL prefilter pass for the trading universe
            #
            tvl_df = client.fetch_tvl(
                mode="min_tvl",
                bucket=TimeBucket.d1,
                start_time=Parameters.backtest_start,
                end_time=Parameters.backtest_end,
                exchange_ids=[exc.exchange_id for exc in targeted_exchanges],
                min_tvl=Parameters.min_tvl,
            )

            tvl_filtered_pair_ids = tvl_df["pair_id"].unique()
            benchmark_pair_ids = [pair_universe.get_pair_by_human_description(desc).pair_id for desc in SUPPORTING_PAIRS]
            needed_pair_ids = set(benchmark_pair_ids) | set(tvl_filtered_pair_ids)
            pairs_df = all_pairs_df[all_pairs_df["pair_id"].isin(needed_pair_ids)]

            dataset = load_partial_data(
                client=client,
                time_bucket=Parameters.candle_time_bucket,
                pairs=pairs_df,
                execution_context=execution_context,
                universe_options=universe_options,
                lending_reserves=LENDING_RESERVES,
                preloaded_tvl_df=tvl_df,
            )

        :param bucket:
            Candle time frame.

            Ask `TimeBucket.d1` or lower. `TimeBucket.m1` is most useful for LP backtesting.

        :param mode:
            Query all exchange data by min_tvl (mode = "min_tvl"), or use given pair list (mode = "pair_ids").

            ``min_tvl`` filters by the highest TVL value. But price because this is easy to manipulate with MEV,
            this often grabs too many pairs that have no real TVL. ``min_tvl_low`` filters by daily TVL low value,
            which often reflects the real TVL better.

        :param exchange_ids:
            Exchange internal ids for min_tvl query.

        :param pair_ids:
            Trading pairs internal ids we query data for.
            Get internal ids from pair dataset.

            Only works with Uniswap v3 pairs.

        :param start_time:
            All candles after this.

            Inclusive.

        :param end_time:
            All candles before this.

            Inclusive.

        :param min_tvl:
            Any pair must have this minimum TVL reached during the start - end period to be included.

            One sided. I.e. only counts /WETH or /USDC in Uniswap v3 pools.

        :param progress_bar_description:
            Display a download progress bar using `tqdm_loggable` if given.

            Set to `None` to disable.

        :return:
            TVL dataframe.

            See :py:mod:`tradingstrategy.clmm` for details.
        """

        assert bucket <= TimeBucket.d1, f"It does not make sense to fetch CLMM data with higher frequency than a 1 day, got {bucket}"

        if isinstance(start_time, pd.Timestamp):
            start_time = start_time.to_pydatetime()

        if isinstance(end_time, pd.Timestamp):
            end_time = end_time.to_pydatetime()

        if progress_bar_description == "...":
            if end_time:
                progress_bar_description = f"Downloading TVL data until {end_time.strftime('%Y-%m-%d')}"
            else:
                progress_bar_description = f"Downloading TVL data"

        return self.transport.fetch_tvl(
            mode=mode,
            time_bucket=bucket,
            pair_ids=pair_ids,
            exchange_ids=exchange_ids,
            min_tvl=min_tvl,
            start_time=start_time,
            end_time=end_time,
            progress_bar_description=progress_bar_description,
        )

    def fetch_trading_data_availability(self,
          pair_ids: Collection[PrimaryKey],
          bucket: TimeBucket,
        ) -> Dict[PrimaryKey, TradingPairDataAvailability]:
        """Check the trading data availability at oracle's real time market feed endpoint.

        - Trading Strategy oracle uses sparse data format where candles
          with zero trades are not generated. This is better suited
          for illiquid DEX markets with few trades.

        - Because of sparse data format, we do not know if there is a last
          candle available - candle may not be available yet or there might not be trades
          to generate a candle

        This endpoint allows to check the trading data availability for multiple of trading pairs.

        Example:

        .. code-block:: python

            exchange_universe = client.fetch_exchange_universe()
            pairs_df = client.fetch_pair_universe().to_pandas()

            # Create filtered exchange and pair data
            exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
            pair_universe = PandasPairUniverse.create_pair_universe(
                    pairs_df,
                    [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD")]
                )

            pair = pair_universe.get_single()

            # Get the latest candle availability for BNB-BUSD pair
            pairs_availability = client.fetch_trading_data_availability({pair.pair_id}, TimeBucket.m15)

        :param pair_ids:
            Trading pairs internal ids we query data for.
            Get internal ids from pair dataset.

        :param time_bucket:
            Candle time frame

        :return:
            Map of pairs -> their trading data availability

        """
        return self.transport.fetch_trading_data_availability(
            pair_ids,
            bucket,
        )

    def fetch_candle_dataset(self, bucket: TimeBucket) -> Path:
        """Fetch candle data from the server.

        Do not attempt to decode the Parquet file to the memory,
        but instead of return raw
        """
        path = self.transport.fetch_candles_all_time(bucket)
        return path
    
    def fetch_lending_candles_by_reserve_id(
        self,
        reserve_id: PrimaryKey,
        bucket: TimeBucket,
        candle_type: LendingCandleType = LendingCandleType.variable_borrow_apr,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
    ) -> pd.DataFrame:
        """Fetch lending candles for a particular reserve.

        :param reserve_id:
            Lending reserve's internal id we query data for.
            Get internal id from lending reserve universe dataset.

        :param bucket:
            Candle time frame.

        :param candle_type:
            Lending candle type.

        :param start_time:
            All candles after this.
            If not given start from genesis.

        :param end_time:
            All candles before this

        :return:
            Lending candles dataframe
        """
        if bucket.to_pandas_timedelta() < pd.Timedelta("1h"):
            bucket = TimeBucket.h1

        return self.transport.fetch_lending_candles_by_reserve_id(
            reserve_id,
            bucket,
            candle_type,
            start_time,
            end_time,
        )

    def fetch_lending_candles_for_universe(
        self,
        lending_reserve_universe: LendingReserveUniverse,
        bucket: TimeBucket,
        candle_types: Collection[LendingCandleType] = (LendingCandleType.variable_borrow_apr, LendingCandleType.supply_apr),
        start_time: datetime.datetime | pd.Timestamp = None,
        end_time: datetime.datetime | pd.Timestamp = None,
        construct_timestamp_column=True,
        progress_bar_description: str | None=None,
    ) -> LendingCandleResult:
        """Load lending reservers for several assets as once.

        - Display a progress bar during download

        - For usage examples see :py:class:`tradingstrategy.lending.LendingCandleUniverse`.

        .. note ::

            This download method is still upoptimised due to small number of reserves

        :param candle_types:
            Data for candle types to load

        :param construct_timestamp_column:
            After loading data, create "timestamp" series based on the index.

            We need to convert index to column if we are going to have
            several reserves in :py:class:`tradingstrategy.lending.LendingCandleUniverse`.

        :param progress_bar_description:
            Override the default progress bar description.

        :return:
            Dictionary of dataframes.

            One DataFrame per candle type we asked for.
        """

        # TODO: Replace the current loaded with JSONL based one to have better progress bar

        assert isinstance(lending_reserve_universe, LendingReserveUniverse)
        assert isinstance(bucket, TimeBucket)
        assert type(candle_types) in (list, tuple,)

        result = {}

        if lending_reserve_universe.get_count() > 30:
            logger.warning("This method is not designed to load data for long list of reserves.\n"
                           "Currently loading data for %s reverses.",
                           lending_reserve_universe.get_count()
                           )


        total = len(candle_types) * lending_reserve_universe.get_count()

        if not progress_bar_description:
            progress_bar_description = "Downloading lending rates"

        with tqdm(desc=progress_bar_description, total=total) as progress_bar:
            # Perform data load by issung several HTTP requests,
            # one for each reserve and candle type
            for candle_type in candle_types:

                bits = []

                for reserve in lending_reserve_universe.iterate_reserves():
                    progress_bar.set_postfix({"Asset": reserve.asset_symbol})
                    try:
                        piece = self.fetch_lending_candles_by_reserve_id(
                            reserve.reserve_id,
                            bucket,
                            candle_type,
                            start_time,
                            end_time,
                        )
                        bits.append(piece)
                    except DataNotAvailable as e:
                        # Some of the reserves do not have full data available yet
                        logger.warning(
                            "Lending candles could not be fetch for reserve: %s, bucket: %s, candle: %s, start: %s, end: %s, error: %s",
                            reserve,
                            bucket,
                            candle_type,
                            start_time,
                            end_time,
                            e,
                        )

                    progress_bar.update()

                if len(bits) == 0:
                    raise DataNotAvailable("No data available for any of the reserves. Check the logs for details.")

                data = pd.concat(bits)

                if construct_timestamp_column:
                    data["timestamp"] = data.index.to_series()

                result[candle_type] = data

        return result

    @_retry_corrupted_parquet_fetch
    def fetch_all_liquidity_samples(self, bucket: TimeBucket) -> Table:
        """Get cached blob of liquidity events of a certain time window.

        The returned data can be between several hundreds of megabytes to several gigabytes
        and is cached locally.

        The returned data is saved in PyArrow Parquet format.
        
        For more information see :py:class:`tradingstrategy.liquidity.XYLiquidity`.

        If the download seems to be corrupted, it will be attempted 3 times.
        """
        path = self.transport.fetch_liquidity_all_time(bucket)
        return read_parquet(path)

    @_retry_corrupted_parquet_fetch
    def fetch_lending_reserve_universe(self) -> LendingReserveUniverse:
        """Load a cache the lending reserve universe.
        """
        path = self.transport.fetch_lending_reserve_universe()

        try:
            return LendingReserveUniverse.from_json(path.read_text())
        except JSONDecodeError as e:
            raise RuntimeError(f"Could not read JSON file {path}") from e

    @_retry_corrupted_parquet_fetch
    def fetch_lending_reserves_all_time(self) -> Table:
        """Get a cached blob of lending protocol reserve events and precomupted stats.

        The returned data can be between several hundreds of megabytes to several
        gigabytes in size, and is cached locally.

        Note that at present the only available data is for the AAVE v3 lending
        protocol.

        The returned data is saved in a PyArrow Parquet format.

        If the download seems to be corrupted, it will be attempted 3 times.
        """
        path = self.transport.fetch_lending_reserves_all_time()
        assert path
        assert os.path.exists(path)
        return read_parquet(path)

    def fetch_chain_status(self, chain_id: ChainId) -> dict:
        """Get live information about how a certain blockchain indexing and candle creation is doing."""
        return self.transport.fetch_chain_status(chain_id.value)

    def fetch_top_pairs(
        self,
        chain_ids: Collection[ChainId],
        exchange_slugs: Collection[str] | None = None,
        addresses: Collection[str] | None = None,
        limit: None = None,
        method: TopPairMethod = TopPairMethod.sorted_by_liquidity_with_filtering,
        min_volume_24h_usd: USDollarAmount | None = 1000,
        risk_score_threshold=65,
    ) -> TopPairsReply:
        """Get new trading pairs to be included in the trading universe.

        **This API is still under heavy development**.

        This endpoint is designed to scan new trading pairs to be included in a trading universe.
        It ranks and filters the daily/weekly/etc. interesting trading pairs by a criteria.

        - Top pairs on exchanges
        - Top pairs for given tokens, by a token address

        The result will include
        - Included and excluded trading pairs
        - Pair metadata
        - Latest volume and liquidity
        - :term:`Token tax` information
        - TokenSniffer risk score

        The result data is asynchronously filled, and may not return the most fresh situation,
        due to data processing delays. So when you call this method `24:00` it does not have
        pairs for yesterday ready yet. The results may vary, but should reflect the look back of last 24h.

        Various heuristics is applied to the result filtering, like excluding stable pairs,
        derivative tokens, choosing the trading pair with the best fee, etc.

        When you store the result, you need to use tuple `(chain id, pool address)` as the persistent key.
        Any integer primary keys may change over long term.

        .. warning::

            Depending on the TokenSniffer data available, this endpoint may take up to 15 seconds per token.

        The endpoint has two modes of operation

        - :py:attr:`TopPairMethod.sorted_by_liquidity_with_filtering`: Give the endpoint a list of exchange slugs and get the best trading pairs on these exchanges. You need to give ``chain_id`, limit` and `exchange_slugs` arguments.
        - :py:attr:`TopPairMethod.by_addresses`: Give the endpoint a list of **token** smart contract addresses and get the best trading pairs for these. You need to give ``chain_id` and `addresses` arguments.

        Example how to get token tax data and the best trading pair for given Ethereum tokens:

        .. code-block:: python

            top_reply = client.fetch_top_pairs(
                chain_ids={ChainId.ethereum},
                addresses={
                    "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",  # COMP
                    "0xc00e94Cb662C3520282E6f5717214004A7f26888"  # AAVE
                },
                method=TopPairMethod.by_token_addresses,
                limit=None,
            )

            assert isinstance(top_reply, TopPairsReply)
            # The top picks will be COMP/WETH and AAVE/WETH based on volume/liquidity
            assert len(top_reply.included) == 2
            # There are many pairs excluded e.g AAVE/USDC and AAVE/USDT) based ones because of low liq/vol
            assert len(top_reply.excluded) > 0

            comp_weth = top_reply.included[0]
            assert comp_weth.base_token == "COMP"
            assert comp_weth.quote_token == "WETH"
            assert comp_weth.get_buy_tax() == 0
            assert comp_weth.get_sell_tax() == 0
            assert comp_weth.volume_24h_usd > 100.0
            assert comp_weth.tvl_latest_usd > 100.0

        Example of chain/exchange based query:

        .. code-block:: python

            # Get top tokens of Uniswap on Ethereum
            top_reply = client.fetch_top_pairs(
                chain_ids={ChainId.ethereum},
                exchange_slugs={"uniswap-v2", "uniswap-v3"},
                limit=10,
            )

            assert isinstance(top_reply, TopPairsReply)
            assert len(top_reply.included) == 10
            assert len(top_reply.excluded) > 0  # There is always something to be excluded

        :param method:
            Currently, hardcoded. No other methods supported.

        :param chain_ids:
            List of blockchains to consider.

            Currently only 1 chain_id supported per query.

        :param exchange_slugs:
            List of DEXes to consider.

        :param addresses:
            List of token addresses to query.

            Token addresses, *not** trading pair addresses.

            The list is designed for base tokens in a trading pair. The list should **not** include any quote tokens like `WETH` or `USDC`
            because the resulting trading pair list is too long to handle, and the server will limit the list at some point.

        :param limit:
            Max number of results.

            If you ask very high number of tokens / pairs, the server will hard limit the response in some point.
            In this case, you may not get a resulting trading pair for a token even if such exists.
            Try to ask max 100 tokens at once.

        :param min_volume_24h_usd:
            Exclude trading pairs that do not reach this volume target.

            The filtered pairs do not appear in the result at all (not worth to load from the database)
            or will appear in `excluded` category.

            Default to $1000. Minimum value is $1.

        :param risk_score_threshold:
            TokenSniffer risk score threshold.

            If the TokenSniffer risk score is below this value, the token will be in `excluded`
            category of the reply, otherwise it is `included`.

        :return:
            Top trading pairs included and excluded in the ranking.

            If `by_addresses` method is used and there is no active trading data for the token,
            the token may not appear in neither `included` or `excluded` results.
        """

        assert len(chain_ids) > 0, f"Got {chain_ids}"
        if method == TopPairMethod.sorted_by_liquidity_with_filtering:
            assert limit, "You must give limit argument with TopPairMethod.sorted_by_liquidity_with_filtering"
            assert len(exchange_slugs) > 0, f"Got {exchange_slugs}"
            assert 1 < limit <= 500
        elif method == TopPairMethod.by_token_addresses:
            assert len(addresses) > 0, f"Got {addresses}"
        else:
            raise NotImplementedError(f"Unknown method {method}")

        data = self.transport.fetch_top_pairs(
            chain_ids=chain_ids,
            exchange_slugs=exchange_slugs,
            limit=limit,
            method=method.value,
            addresses=addresses,
            min_volume_24h_usd=min_volume_24h_usd,
            risk_score_threshold=risk_score_threshold,
        )
        return TopPairsReply.from_dict(data)

    def fetch_token_metadata(
        self,
        chain_id: ChainId,
        addresses: Collection[str],
    ) -> dict[str, TokenMetadata]:
        """Get token metadata for several tokens.

        - Cached locally on disk if possible

        - If there is no known token, the resulting dict does not contain entry for this address

        - Also if the token data is broken/not serialisable for some reason, the token might not appear in the output

        :return:
            Address -> metadata mapping.

            All addresses will be in lowercase.
        """
        return self.transport.fetch_token_metadata(
            chain_id=chain_id,
            addresses=addresses,
            progress_bar_description="Loading token metadata",
        )

    @classmethod
    def preflight_check(cls):
        """Checks that everything is in ok to run the notebook"""

        # Work around Google Colab shipping with old Pandas
        # https://stackoverflow.com/questions/11887762/how-do-i-compare-version-numbers-in-python
        import pandas
        from packaging import version
        pandas_version = version.parse(pandas.__version__)
        assert pandas_version >= version.parse("1.3"), f"Pandas 1.3.0 or greater is needed. You have {pandas.__version__}. If you are running this notebook in Google Colab and this is the first run, you need to choose Runtime > Restart and run all from the menu to force the server to load newly installed version of Pandas library."

    @classmethod
    def setup_notebook(cls):
        """Legacy."""
        warnings.warn('This method is deprecated. Use tradeexecutor.utils.notebook module', DeprecationWarning, stacklevel=2)
        # https://stackoverflow.com/a/51955985/315168
        try:
            import matplotlib as mpl
            mpl.rcParams['figure.dpi'] = 600
        except ImportError:
            pass

    @classmethod
    async def create_pyodide_client_async(cls,
                                    cache_path: Optional[str] = None,
                                    api_key: Optional[str] = PYODIDE_API_KEY,
                                    remember_key=False) -> "Client":
        """Create a new API client inside Pyodide enviroment.

        `More information about Pyodide project / running Python in a browser <https://pyodide.org/>`_.

        :param cache_path:
            Virtual file system path

        :param cache_api_key:
            The API key used with the server downloads.
            A special hardcoded API key is used to identify Pyodide
            client and its XmlHttpRequests. A referral
            check for these requests is performed.

        :param remember_key:
            Store the API key in IndexDB for the future use

        :return:
            pass
        """
        from tradingstrategy.environment.jupyterlite import IndexDB

        # Store API
        if remember_key:

            db = IndexDB()

            if api_key:
                await db.set_file("api_key", api_key)

            else:
                api_key = await db.get_file("api_key")

        return cls.create_jupyter_client(cache_path, api_key, pyodide=True)

    @classmethod
    def create_jupyter_client(
        cls,
        cache_path: Optional[str] = None,
        api_key: Optional[str] = None,
        pyodide=None,
        settings_path=DEFAULT_SETTINGS_PATH,
    ) -> "Client":
        """Create a new API client.

        This function is intended to be used from Jupyter notebooks

        .. note ::

            Only use within Jupyter Notebook environments. Otherwise use :py:meth:`create_live_client`.

        - Any local or server-side IPython session

        - JupyterLite notebooks

        :param api_key:
            If not given, do an interactive API key set up in the Jupyter notebook
            while it is being run.

        :param cache_path:
            Where downloaded datasets are stored. Defaults to `~/.cache`.

        :param pyodide:
            Detect the use of this library inside Pyodide / JupyterLite.
            If `None` then autodetect Pyodide presence,
            otherwise can be forced with `True`.

        :param settings_path:
            Where do we write our settings file.

            Set ``None`` to disable settings file in Docker/web browser environments.

        """

        from tradingstrategy.transport.progress_enabled_download import download_with_tqdm_progress_bar

        if pyodide is None:
            try:
                from tradingstrategy.utils.jupyter import is_pyodide
                pyodide = is_pyodide()
            except ImportError:
                pyodide = False

        cls.preflight_check()
        env = DefaultClientEnvironment(settings_path=settings_path)

        # Try Pyodide default key
        if not api_key:
            if pyodide:
                api_key = PYODIDE_API_KEY

        # Try file system stored API key,
        # if not prompt interactively
        if not api_key:
            assert settings_path, \
                "Trading Strategy API key not given as TRADING_STRATEGY_API_KEY environment variable or an argument.\n" \
                "Interactive setup is disabled for this data client.\n" \
                "Cannot continue."

            config = env.setup_on_demand(api_key=api_key)
            api_key = config.api_key

        cache_path = cache_path or env.get_cache_path()

        transport = CachedHTTPTransport(
            download_with_tqdm_progress_bar,
            cache_path=cache_path,
            api_key=api_key)
        return Client(env, transport)

    @classmethod
    def create_test_client(cls, cache_path=None, timeout=DEFAULT_TIMEOUT) -> "Client":
        """Create a new Trading Strategy client to be used with automated test suites.

        Reads the API key from the environment variable `TRADING_STRATEGY_API_KEY`.
        A temporary folder is used as a cache path.

        By default, the test client caches data under `/tmp` folder.
        Tests do not clear this folder between test runs, to make tests faster.
        """

        if cache_path:
            os.makedirs(cache_path, exist_ok=True)
        else:
            cache_path = tempfile.mkdtemp()

        api_key = os.environ.get("TRADING_STRATEGY_API_KEY")
        assert api_key, "Unit test data client cannot be created without TRADING_STRATEGY_API_KEY env"

        env = DefaultClientEnvironment(cache_path=cache_path, settings_path=None)
        config = Configuration(api_key=api_key)
        transport = CachedHTTPTransport(
            download_with_progress_plain,
            "https://tradingstrategy.ai/api",
            api_key=config.api_key,
            cache_path=env.get_cache_path(),
            timeout=timeout,  # Likely first timeouter /tvl endpoint
        )
        return Client(env, transport)

    @classmethod
    def create_live_client(
        cls,
        api_key: Optional[str] = None,
        cache_path: Optional[Path] = None,
        settings_path: Path | None = DEFAULT_SETTINGS_PATH,
        timeout=DEFAULT_TIMEOUT,
    ) -> "Client":
        """Create a live trading instance of the client.

        - The live client is non-interactive and logs using Python logger

        - If you want to run inside notebook, use :py:meth:`create_jupyter_client` instead

        Example:

        .. code-block:: python

            from tradingstrategy.chain import ChainId
            from tradingstrategy.client import Client
            from tradingstrategy.pair import PandasPairUniverse
            from tradingstrategy.timebucket import TimeBucket

            # Disable the settings file.
            # API key must be given in an environment variable.
            client = Client.create_live_client(
                settings_path=None,
                api_key=os.environ["TRADING_STRATEGY_API_KEY"],
            )
            # Load pairs in all exchange
            exchange_universe = client.fetch_exchange_universe()
            pairs_df = client.fetch_pair_universe().to_pandas()

            pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

            pair_ids = [
                pair_universe.get_pair_by_human_description([ChainId.ethereum, "uniswap-v3", "WETH", "USDC", 0.0005]).pair_id,
            ]

            start = pd.Timestamp.utcnow() - pd.Timedelta("3d")
            end = pd.Timestamp.utcnow()

            # Download some data
            clmm_df = client.fetch_clmm_liquidity_provision_candles_by_pair_ids(
                pair_ids,
                TimeBucket.d1,
                start_time=start,
                end_time=end,
            )

        :param api_key:
            Trading Strategy oracle API key, starts with `secret-token:tradingstrategy-...`

        :param cache_path:
            Where downloaded datasets are stored. Defaults to `~/.cache`.

        :param settings_path:
            Where do we write our settings file.

            Set ``None`` to disable settings file in Docker environments.
        """

        cls.preflight_check()

        if settings_path is None:
            assert api_key, "Either API key or settings file must be given"

        env = DefaultClientEnvironment(settings_path=settings_path)
        if cache_path:
            cache_path = cache_path.as_posix()
        else:
            cache_path = env.get_cache_path()

        config = Configuration(api_key)

        transport = CachedHTTPTransport(
            download_with_progress_plain,
            "https://tradingstrategy.ai/api",
            cache_path=cache_path,
            api_key=config.api_key,
            add_exception_hook=False,
            timeout=DEFAULT_TIMEOUT,
        )

        return Client(env, transport)
