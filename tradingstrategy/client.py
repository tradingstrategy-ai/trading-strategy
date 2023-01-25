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
from functools import wraps
from json import JSONDecodeError
from pathlib import Path
from typing import Final, Optional, Set, Union, Collection, Dict

# TODO: Must be here because  warnings are very inconveniently triggered import time
import pandas as pd
from tqdm import TqdmExperimentalWarning

from tradingstrategy.candle import TradingPairDataAvailability
# "Using `tqdm.autonotebook.tqdm` in notebook mode. Use `tqdm.tqdm` instead to force console mode (e.g. in jupyter console) from tqdm.autonotebook import tqdm"
from tradingstrategy.reader import BrokenData, read_parquet
from tradingstrategy.transport.pyodide import PYODIDE_API_KEY
from tradingstrategy.types import PrimaryKey
from tradingstrategy.utils.jupyter import is_pyodide

warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)


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
from tradingstrategy.environment.jupyter import (
    JupyterEnvironment,
    download_with_tqdm_progress_bar,
)
from tradingstrategy.exchange import ExchangeUniverse
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.transport.cache import CachedHTTPTransport


logger = logging.getLogger(__name__)


RETRY_DELAY: Final[int] = 30  # seconds

MAX_ATTEMPTS: Final[int] = 3


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
                    logger.warning(
                        f"Exhausted all {MAX_ATTEMPTS} attempts, fetching parquet data failed."
                    )
                    self.clear_caches(filename=path_to_remove)
                    raise

    return impl


class Client:
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
        path = self.transport.fetch_pair_universe()
        return read_parquet(path)

    def fetch_exchange_universe(self) -> ExchangeUniverse:
        """Fetch list of all exchanges form the :term:`dataset server`.
        """
        path = self.transport.fetch_exchange_universe()
        with path.open("rt", encoding="utf-8") as inp:
            try:
                return ExchangeUniverse.from_json(inp.read())
            except JSONDecodeError as e:
                raise RuntimeError(f"Could not read JSON file {path}") from e

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
        return read_parquet(path)

    def fetch_candles_by_pair_ids(self,
          pair_ids: Collection[PrimaryKey],
          bucket: TimeBucket,
          start_time: Optional[datetime.datetime] = None,
          end_time: Optional[datetime.datetime] = None,
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
        return self.transport.fetch_candles_by_pair_ids(
            pair_ids,
            bucket,
            start_time,
            end_time,
            max_bytes=max_bytes,
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
            pair_universe = PandasPairUniverse.create_single_pair_universe(
                    pairs_df,
                    exchange,
                    "WBNB",
                    "BUSD",
                    pick_by_highest_vol=True,
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

    def fetch_chain_status(self, chain_id: ChainId) -> dict:
        """Get live information about how a certain blockchain indexing and candle creation is doing."""
        return self.transport.fetch_chain_status(chain_id.value)

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
        """Setup diagram rendering and such.

        Force high DPI output for all images.
        """
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
    def create_jupyter_client(cls,
                              cache_path: Optional[str] = None,
                              api_key: Optional[str] = None,
                              pyodide=None,
                              ) -> "Client":
        """Create a new API client.

        This function is intented to be used from Jupyter notebooks

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
        """

        if pyodide is None:
            pyodide = is_pyodide()

        cls.preflight_check()
        cls.setup_notebook()
        env = JupyterEnvironment()

        # Try Pyodide default key
        if not api_key:
            if pyodide:
                api_key = PYODIDE_API_KEY

        # Try file system stored API key,
        # if not prompt interactively
        if not api_key:
            config = env.setup_on_demand(api_key=api_key)
            api_key = config.api_key

        cache_path = cache_path or env.get_cache_path()
        transport = CachedHTTPTransport(
            download_with_tqdm_progress_bar,
            cache_path=cache_path,
            api_key=api_key)
        return Client(env, transport)

    @classmethod
    def create_test_client(cls, cache_path=None) -> "Client":
        """Create a new Capitalgram clienet to be used with automated test suites.

        Reads the API key from the environment variable `TRADING_STRATEGY_API_KEY`.
        A temporary folder is used as a cache path.

        By default, the test client caches data under `/tmp` folder.
        Tests do not clear this folder between test runs, to make tests faster.
        """
        if cache_path:
            os.makedirs(cache_path, exist_ok=True)
        else:
            cache_path = tempfile.mkdtemp()

        env = JupyterEnvironment(cache_path=cache_path)
        config = Configuration(api_key=os.environ["TRADING_STRATEGY_API_KEY"])
        transport = CachedHTTPTransport(download_with_progress_plain, "https://tradingstrategy.ai/api", api_key=config.api_key, cache_path=env.get_cache_path(), timeout=15)
        return Client(env, transport)

    @classmethod
    def create_live_client(cls, api_key: Optional[str]=None, cache_path: Optional[Path]=None) -> "Client":
        """Create a live trading instance of the client.

        The live client is non-interactive and logs using Python logger.

        :param api_key: Trading Strategy oracle API key, starts with `secret-token:tradingstrategy-...`

        :param cache_path: Where downloaded datasets are stored. Defaults to `~/.cache`.
        """
        cls.preflight_check()
        cls.setup_notebook()
        env = JupyterEnvironment()
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
            add_exception_hook=False)
        return Client(env, transport)
