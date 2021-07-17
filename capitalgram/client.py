import json
from typing import List

import pyarrow as pa
import zstandard as zstd

from capitalgram.candle import CandleBucket
from capitalgram.exchange import Exchange, ExchangeUniverse
from capitalgram.reader import read_parquet
from capitalgram.chain import ChainId
from capitalgram.environment.base import Environment
from capitalgram.environment.jupyter import JupyterEnvironment
from capitalgram.pair import PairUniverse
from capitalgram.transport.cache import CachedHTTPTransport


class Capitalgram:
    """An API client for querying the Capitalgram candle server.

    This client will download and manage cached datasets.
    """

    @classmethod
    def create_jupyter_client(cls, cache_path=None) -> "Capitalgram":
        """Create a new API client.

        :param cache_path: Where downloaded datasets are stored. Defaults to `~/.cache`.
        """
        return Capitalgram(JupyterEnvironment(), CachedHTTPTransport("https://candlelightdinner.capitalgram.com", cache_path=cache_path))

    def __init__(self, env: Environment, transport: CachedHTTPTransport):
        """Do not call constructor directly, but use one of create methods. """
        self.env = env
        self.transport = transport

    def start(self):
        """Checks the API key validity and if the server is responsive.

        If no API key is avaiable open an interactive prompt to register (if available)
        """
        pass

    def register_on_demand(self):
        """If there is not yet API key available, automatically register for one."""
        pass

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

