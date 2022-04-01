import datetime
import io
import os
import pathlib
from typing import Optional, Callable
import shutil
import logging

import requests
from requests import Response

from tradingstrategy.timebucket import TimeBucket

logger = logging.getLogger(__name__)


class APIError(Exception):
    pass


class CachedHTTPTransport:
    """Download live and cached datasets from the candle server and cache locally.

    The download files are very large and expect to need several gigabytes of space for them.
    """

    def __init__(self,
                 download_func: Callable,
                 endpoint: Optional[str] = None,
                 cache_period =datetime.timedelta(days=3),
                 cache_path: Optional[str] = None,
                 api_key: Optional[str] = None,
                 timeout: float = 15.0):
        """
        :param download_func: Interactive download progress bar displayed during the download
        :param endpoint: API server we are using - default is `https://tradingstrategy.ai/api`
        :param cache_period: How many days we store the downloaded files
        :param cache_path: Where we store the downloaded files
        :param api_key: Trading Strategy API key to use download
        :param timeout: requests HTTP lib timeout
        """

        self.download_func = download_func

        if endpoint:
            self.endpoint = endpoint
        else:
            self.endpoint = "https://tradingstrategy.ai/api"

        self.cache_period = cache_period

        if cache_path:
            self.cache_path = cache_path
        else:
            self.cache_path = os.path.expanduser("~/.cache/trading-strategy")

        self.requests = self.create_requests_client(api_key=api_key)

        self.api_key = api_key
        self.timeout = timeout

    def create_requests_client(self, api_key: Optional[str] = None):
        """Create HTTP 1.1 keep-alive connection to the server with optional authorization details."""

        session = requests.Session()

        if api_key:
            session.headers.update({'Authorization': api_key})

        def exception_hook(response: Response, *args, **kwargs):
            if response.status_code >= 400:
                raise APIError(f"Server error reply: code:{response.status_code} message:{response.text}")

        session.hooks = {
            "response": exception_hook,
        }
        return session

    def get_abs_cache_path(self):
        return os.path.abspath(self.cache_path)

    def get_cached_file_path(self, fname):
        path = os.path.join(self.get_abs_cache_path(), fname)
        return path

    def get_cached_item(self, fname) -> Optional[pathlib.Path]:

        path = self.get_cached_file_path(fname)
        if not os.path.exists(path):
            return None

        f = pathlib.Path(path)
        mtime = datetime.datetime.fromtimestamp(f.stat().st_mtime)
        if datetime.datetime.now() - mtime > self.cache_period:
            # File cache expired
            return None

        return f

    def purge_cache(self):
        """Delete all cached files on the filesystem."""
        if os.path.exists(self.cache_path):
            logger.info("Purging caches at %s", self.cache_path)
            shutil.rmtree(self.cache_path)

    def save_response(self, fpath, api_path, params=None):
        """Download a file to the cache and display a pretty progress bar while doing it."""
        os.makedirs(self.get_abs_cache_path(), exist_ok=True)
        url = f"{self.endpoint}/{api_path}"
        logger.debug("Saving %s to %s", url, fpath)
        # https://stackoverflow.com/a/14114741/315168
        self.download_func(self.requests, fpath, url, params, self.timeout)

    def get_json_response(self, api_path, params=None):
        url = f"{self.endpoint}/{api_path}"
        response = self.requests.get(url, params=params)
        return response.json()

    def post_json_response(self, api_path, params=None):
        url = f"{self.endpoint}/{api_path}"
        response = self.requests.post(url, params=params)
        return response.json()

    def fetch_chain_status(self, chain_id: int) -> dict:
        """Not cached."""
        return self.get_json_response("chain-status", params={"chain_id": chain_id})

    def fetch_pair_universe(self) -> pathlib.Path:
        fname = "pair-universe.parquet"
        cached = self.get_cached_item(fname)
        if cached:
            return cached

        # Download save the file
        path = self.get_cached_file_path(fname)
        self.save_response(path, "pair-universe")
        return self.get_cached_item(fname)

    def fetch_exchange_universe(self) -> pathlib.Path:
        fname = "exchange-universe.json"
        cached = self.get_cached_item(fname)
        if cached:
            return cached

        # Download save the file
        path = self.get_cached_file_path(fname)
        self.save_response(path, "exchange-universe")
        return self.get_cached_item(fname)

    def fetch_candles_all_time(self, bucket: TimeBucket) -> pathlib.Path:
        assert isinstance(bucket, TimeBucket)
        fname = f"candles-{bucket.value}.parquet"
        cached = self.get_cached_item(fname)
        if cached:
            return cached
        # Download save the file
        path = self.get_cached_file_path(fname)
        self.save_response(path, "candles-all", params={"bucket": bucket.value})
        return self.get_cached_item(path)

    def fetch_liquidity_all_time(self, bucket: TimeBucket) -> pathlib.Path:
        fname = f"liquidity-samples-{bucket.value}.parquet"
        cached = self.get_cached_item(fname)
        if cached:
            return cached
        # Download save the file
        path = self.get_cached_file_path(fname)
        self.save_response(path, "liquidity-all", params={"bucket": bucket.value})
        return self.get_cached_item(path)

    def ping(self) -> dict:
        reply = self.get_json_response("ping")
        return reply

    def message_of_the_day(self) -> dict:
        reply = self.get_json_response("message-of-the-day")
        return reply

    def register(self, first_name, last_name, email) -> dict:
        """Makes a register request.

        The request does not load any useful payload, but it is assumed the email message gets verified
        and the user gets the API from the email.
        """
        reply = self.post_json_response("register", params={"first_name": first_name, "last_name": last_name, "email": email})
        return reply
