import datetime
import io
import os
import pathlib
import time
from typing import List, Optional
import shutil
import logging

import requests
from requests import Response

from capitalgram.candle import CandleBucket


logger = logging.getLogger(__name__)

class APIError(Exception):
    pass


class CachedHTTPTransport:
    """Download live and cached datasets from the candle server and cache locally.

    The download files are very large and expect to need several gigabytes of space for them.
    """

    def __init__(self, endpoint: str, cache_period=datetime.timedelta(days=3), cache_path: Optional[str]=None):
        self.endpoint = endpoint
        self.cache_period = cache_period

        if cache_path:
            self.cache_path = cache_path
        else:
            self.cache_path = os.path.expanduser("~/.cache/capitalgram")

        self.requests = self.create_requests_client()

    def create_requests_client(self):
        session = requests.Session()

        def exception_hook(response: Response, *args, **kwargs):
            if response.status_code >= 400:
                raise APIError(response.text)

        session.hooks = {
            "response": exception_hook,
        }
        return session

    def get_abs_cache_path(self):
        return os.path.abspath(self.cache_path)

    def get_cached_file_path(self, fname):
        path = os.path.join(self.get_abs_cache_path(), fname)
        return path

    def get_cached_item(self, fname) -> Optional[io.BytesIO]:

        path = self.get_cached_file_path(fname)
        if not os.path.exists(path):
            return None

        f = pathlib.Path(path)
        mtime = datetime.datetime.fromtimestamp(f.stat().st_mtime)
        if datetime.datetime.now() - mtime > self.cache_period:
            # File cache expired
            return None

        return open(path, "rb")

    def purge_cache(self):
        """Delete all cached files on the filesystem."""
        shutil.rmtree(self.cache_period)

    def save_response(self, fpath, api_path, params=None):
        url = f"{self.endpoint}/{api_path}"
        # https://stackoverflow.com/a/14114741/315168
        start = time.time()
        response = self.requests.get(url, params=params)
        fsize = 0
        with open(fpath, 'wb') as handle:
            for block in response.iter_content(8 * 1024):
                handle.write(block)
                fsize += len(block)
        duration = time.time() - start
        logger.debug("Saved %s. Downloaded %s bytes in %f seconds.", fpath, fsize, duration)

    def get_json_response(self, api_path, params=None):
        url = f"{self.endpoint}/{api_path}"
        response = self.requests.get(url, params=params)
        return response.json()

    def fetch_chain_status(self, chain_id: int) -> dict:
        """Not cached."""
        return self.get_json_response("chain-status", params={"chain_id": chain_id})

    def fetch_pair_universe(self) -> io.BytesIO:
        fname = "pair-universe.json.zstd"
        cached = self.get_cached_item(fname)
        if cached:
            return cached

        # Download save the file
        path = self.get_cached_file_path(fname)
        self.save_response(path, "pair-universe")
        return self.get_cached_item(fname)

    def fetch_exchange_universe(self) -> io.BytesIO:
        fname = "exchange-universe.json"
        cached = self.get_cached_item(fname)
        if cached:
            return cached

        # Download save the file
        path = self.get_cached_file_path(fname)
        self.save_response(path, "exchanges")
        return self.get_cached_item(fname)

    def fetch_candles_all_time(self, bucket: CandleBucket) -> io.BytesIO:
        fname = f"candles-{bucket.value}.feather"
        cached = self.get_cached_item(fname)
        if cached:
            return cached
        # Download save the file
        path = self.get_cached_file_path(fname)
        self.save_response(path, "candles-all", params={"bucket": bucket.value})
        return self.get_cached_item(path)