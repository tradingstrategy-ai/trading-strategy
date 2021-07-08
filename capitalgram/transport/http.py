import datetime
from typing import List

import requests

from capitalgram.caip import ChainAddressTuple
from capitalgram.pair import PairUniverse


class HTTPTransport:
    """Fetches data over HTTP and JSON APIs."""

    def __init__(self, endpoint="https://candlelightdinner.capitalgram.com"):
        self.session = requests.Session()
        self.endpoint = endpoint

    def fetch_stats(self) -> dict:
        self.session.get("")

    def fetch_pair_universe(self) -> PairUniverse:
        pass

    def fetch_candles(self, pair_list: List[ChainAddressTuple], start: datetime.datetime, end: datetime.datetime):
        """Downlaod pair universe data."""