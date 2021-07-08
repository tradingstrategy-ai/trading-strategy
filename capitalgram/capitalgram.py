import datetime
from typing import List

from capitalgram.caip import ChainAddressTuple
from capitalgram.chain import ChainId
from capitalgram.pair import PairUniverse


class Capitalgram:
    """An API client for Capitalgram quant service.

    """

    def __init__(self):
        pass

    def start(self):
        """Checks the API key validity and if the server is responsive.

        If no API key is avaiable open an interactive prompt to register (if available)
        """

    def register_on_demand(self):
        """If there is not yet API key available, automatically register for one."""

    def fetch_chain_stats(self, chain_id: ChainId):
        """Get candle server report on chain indexing status."""

    def fetch_pair_universe(self, universe: PairUniverse):
        """Downlaod pair universe data."""

    def fetch_candles(self, pair_list: List[ChainAddressTuple], start: datetime.datetime, end: datetime.datetime):
        """Downlaod pair universe data."""