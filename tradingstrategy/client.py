import os
import tempfile
from typing import Optional
from pathlib import Path

# TODO: Must be here because  warnings are very inconveniently triggered import time
from tqdm import TqdmExperimentalWarning
import warnings
# "Using `tqdm.autonotebook.tqdm` in notebook mode. Use `tqdm.tqdm` instead to force console mode (e.g. in jupyter console) from tqdm.autonotebook import tqdm"
warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)

import pyarrow
import pyarrow as pa
from pyarrow import Table

from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.environment.config import Configuration
from tradingstrategy.exchange import ExchangeUniverse
from tradingstrategy.reader import read_parquet
from tradingstrategy.chain import ChainId
from tradingstrategy.environment.base import Environment, download_with_progress_plain
from tradingstrategy.environment.jupyter import JupyterEnvironment, download_with_progress_jupyter
from tradingstrategy.transport.cache import CachedHTTPTransport


class Client:
    """An API client for querying the Capitalgram candle server.

    This client will download and manage cached datasets.
    """

    def __init__(self, env: Environment, transport: CachedHTTPTransport):
        """Do not call constructor directly, but use one of create methods. """
        self.env = env
        self.transport = transport

    def clear_caches(self):
        """Remove any cached data.

        Cache is specific to the current transport.
        """
        self.transport.purge_cache()

    def fetch_pair_universe(self) -> pa.Table:
        """Fetch pair universe from local cache or the candle server.

        The compressed file size is around 5 megabytes.
        """
        stream = self.transport.fetch_pair_universe()
        return read_parquet(stream)

    def fetch_exchange_universe(self) -> ExchangeUniverse:
        """Fetch list of all exchanges form the :term:`dataset server`.
        """
        stream = self.transport.fetch_exchange_universe()
        data = stream.read()
        return ExchangeUniverse.from_json(data)

    def fetch_all_candles(self, bucket: TimeBucket) -> pyarrow.Table:
        """Get cached blob of candle data of a certain candle width.

        The returned data can be between several hundreds of megabytes to several gigabytes
        and is cached locally.

        The returned data is saved in PyArrow Parquet format.

        For more information see :py:class:`tradingstrategy.candle.Candle`.
        """
        stream = self.transport.fetch_candles_all_time(bucket)
        return read_parquet(stream)

    def fetch_all_liquidity_samples(self, bucket: TimeBucket) -> Table:
        """Get cached blob of liquidity events of a certain time window.

        The returned data can be between several hundreds of megabytes to several gigabytes
        and is cached locally.

        The returned data is saved in PyArrow Parquet format.
        
        For more information see :py:class:`tradingstrategy.liquidity.XYLiquidity`.
        """
        stream = self.transport.fetch_liquidity_all_time(bucket)
        return read_parquet(stream)

    def fetch_chain_status(self, chain_id: ChainId) -> dict:
        """Get live information about how a certain blockchain indexing and candle creation is doing."""
        return self.transport.fetch_chain_status(chain_id.value)

    @classmethod
    def preflight_check(cls):
        """Checks that everything is in ok to run the notebook"""

        # Work around Google Colab shipping with old Pandas
        # https://stackoverflow.com/questions/11887762/how-do-i-compare-version-numbers-in-python
        from packaging import version
        import pandas
        pandas_version = version.parse(pandas.__version__)
        assert pandas_version >= version.parse("1.3"), f"Pandas 1.3.0 or greater is needed. You have {pandas.__version__}. If you are running this notebook in Google Colab and this is the first run, you need to choose Runtime > Restart and run all from the menu to force the server to load newly installed version of Pandas library."

        # Fix Backtrader / Pandas 1.3 issue that breaks FastQuant
        try:
            import fastquant
            fastquant_enabled = True
        except ImportError:
            fastquant_enabled = False

        if fastquant_enabled:
            from tradingstrategy.frameworks.fastquant_monkey_patch import apply_patch
            apply_patch()

    @classmethod
    def setup_notebook(cls):
        """Setup diagram rendering and such.

        Force high DPI output for all images.
        """
        # https://stackoverflow.com/a/51955985/315168
        import matplotlib as mpl
        mpl.rcParams['figure.dpi'] = 600

    @classmethod
    def create_jupyter_client(cls, cache_path: Optional[str]=None, api_key: Optional[str]=None) -> "Client":
        """Create a new API client.

        :param cache_path: Where downloaded datasets are stored. Defaults to `~/.cache`.
        """
        cls.preflight_check()
        cls.setup_notebook()
        env = JupyterEnvironment()
        config = env.setup_on_demand()
        transport = CachedHTTPTransport(download_with_progress_jupyter, cache_path=env.get_cache_path(), api_key=config.api_key)
        return Client(env, transport)

    @classmethod
    def create_test_client(cls, cache_path=None) -> "Client":
        """Create a new Capitalgram clienet to be used with automated test suites.

        Reads the API key from the environment variable `TRADING_STRATEGY_API_KEY`.
        A temporary folder is used as a cache path.
        """
        if cache_path:
            os.makedirs(cache_path, exist_ok=True)
        else:
            cache_path = tempfile.mkdtemp()

        env = JupyterEnvironment(cache_path=cache_path)
        config = Configuration(api_key=os.environ["TRADING_STRATEGY_API_KEY"])
        transport = CachedHTTPTransport(download_with_progress_plain, "https://tradingstrategy.ai/api", api_key=config.api_key, cache_path=env.get_cache_path(), timeout=60)
        return Client(env, transport)

    @classmethod
    def create_live_client(cls, api_key: Optional[str]=None, cache_path: Optional[Path]=None) -> "Client":
        """Create a live trading instance of the client.

        The live client is non-interactive and logs using Python logger.

        :param api_key: Trading Strategy oracle API key, starts with ``

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
        transport = CachedHTTPTransport(download_with_progress_plain, "https://tradingstrategy.ai/api", cache_path=cache_path, api_key=config.api_key)
        return Client(env, transport)
