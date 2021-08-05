import os
import tempfile
from typing import Optional

# TODO: Must be here because  warnings are very inconveniently triggered import time
from tqdm import TqdmExperimentalWarning
import warnings
# "Using `tqdm.autonotebook.tqdm` in notebook mode. Use `tqdm.tqdm` instead to force console mode (e.g. in jupyter console) from tqdm.autonotebook import tqdm"
warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)

import pyarrow
import pyarrow as pa
from pyarrow import Table

from capitalgram.timebucket import TimeBucket
from capitalgram.environment.config import Configuration
from capitalgram.exchange import ExchangeUniverse
from capitalgram.reader import read_parquet
from capitalgram.chain import ChainId
from capitalgram.environment.base import Environment, download_with_progress_plain
from capitalgram.environment.jupyter import JupyterEnvironment, download_with_progress_jupyter
from capitalgram.transport.cache import CachedHTTPTransport


class Capitalgram:
    """An API client for querying the Capitalgram candle server.

    This client will download and manage cached datasets.
    """

    def __init__(self, env: Environment, transport: CachedHTTPTransport):
        """Do not call constructor directly, but use one of create methods. """
        self.env = env
        self.transport = transport

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

        For more information see :py:class:`capitalgram.candle.Candle`.
        """
        stream = self.transport.fetch_candles_all_time(bucket)
        return read_parquet(stream)

    def fetch_all_liquidity_samples(self, bucket: TimeBucket) -> Table:
        """Get cached blob of liquidity events of a certain time window.

        The returned data can be between several hundreds of megabytes to several gigabytes
        and is cached locally.

        The returned data is saved in PyArrow Parquet format.
        
        For more information see :py:class:`capitalgram.liquidity.XYLiquidity`.
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
        from capitalgram.frameworks.fastquant_monkey_patch import apply_patch
        apply_patch()

    @classmethod
    def create_jupyter_client(cls, cache_path: Optional[str]=None, api_key: Optional[str]=None) -> "Capitalgram":
        """Create a new API client.

        :param cache_path: Where downloaded datasets are stored. Defaults to `~/.cache`.
        """

        cls.preflight_check()

        env = JupyterEnvironment()
        config = env.setup_on_demand()
        transport = CachedHTTPTransport(download_with_progress_jupyter, cache_path=env.get_cache_path(), api_key=config.api_key)
        return Capitalgram(env, transport)

    @classmethod
    def create_test_client(cls, cache_path=None) -> "Capitalgram":
        """Create a new Capitalgram clienet to be used with automated test suites.

        Reads the API key from the environment variable `CAPITALGRAM_API_KEY`.
        A temporary folder is used as a cache path.
        """
        if cache_path:
            os.makedirs(cache_path, exist_ok=True)
        else:
            cache_path = tempfile.mkdtemp()

        env = JupyterEnvironment(cache_path=cache_path)
        config = Configuration(api_key=os.environ["CAPITALGRAM_API_KEY"])
        transport = CachedHTTPTransport(download_with_progress_plain, "https://candlelightdinner.capitalgram.com", api_key=config.api_key, cache_path=env.get_cache_path())
        return Capitalgram(env, transport)
