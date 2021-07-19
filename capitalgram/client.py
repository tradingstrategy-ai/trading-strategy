import os
import tempfile
from typing import Optional

import pyarrow as pa

from capitalgram.candle import CandleBucket
from capitalgram.environment.colab import ColabEnvironment
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

    @classmethod
    def create_jupyter_client(cls, cache_path: Optional[str]=None, api_key: Optional[str]=None) -> "Capitalgram":
        """Create a new API client.

        :param cache_path: Where downloaded datasets are stored. Defaults to `~/.cache`.
        """
        env = JupyterEnvironment()
        config = env.setup_on_demand()
        transport = CachedHTTPTransport(download_with_progress_jupyter, cache_path=env.get_cache_path(), api_key=config.api_key)
        return Capitalgram(env, transport)

    @classmethod
    def create_test_client(cls) -> "Capitalgram":
        """Create a new Capitalgram clienet to be used with automated test suites.

        Reads the API key from the environment variable `CAPITALGRAM_API_KEY`.
        A temporary folder is used as a cache path.
        """
        cache_path = tempfile.mkdtemp()
        env = JupyterEnvironment(cache_path=cache_path)
        config = Configuration(api_key=os.environ["CAPITALGRAM_API_KEY"])
        transport = CachedHTTPTransport(download_with_progress_plain, "https://candlelightdinner.capitalgram.com", api_key=config.api_key, cache_path=env.get_cache_path())
        return Capitalgram(env, transport)

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

    def fetch_all_candles(self, bucket: CandleBucket) -> pa.Table:
        """Get cached blob of candle data of a certain candle width.

        The returned data can be between several hundreds of megabytes to several gigabytes
        and is cached locally.

        The returned data is saved in Pyarror Feather format.
        """
        stream = self.transport.fetch_candles_all_time(bucket)
        return read_parquet(stream)

    def fetch_chain_status(self, chain_id: ChainId) -> dict:
        """Get live information about how a certain blockchain indexing and candle creation is doing."""
        return self.transport.fetch_chain_status(chain_id.value)

