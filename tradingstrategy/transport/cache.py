import datetime
import hashlib
import os
import pathlib
import re
from typing import Optional, Callable, Set, Union
import shutil
import logging

import pandas
import pandas as pd
import requests
from requests import Response

from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.transport.jsonl import load_candles_jsonl

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
                 timeout: float = 15.0,
                 add_exception_hook=True):
        """
        :param download_func: Interactive download progress bar displayed during the download
        :param endpoint: API server we are using - default is `https://tradingstrategy.ai/api`
        :param cache_period: How many days we store the downloaded files
        :param cache_path: Where we store the downloaded files
        :param api_key: Trading Strategy API key to use download
        :param timeout: requests HTTP lib timeout
        :param add_exception_hook: Automatically raise an error in the case of HTTP error. Prevents auto retries.
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

    def close(self):
        """Release any underlying sockets."""
        self.requests.close()

    def create_requests_client(self, api_key: Optional[str] = None, add_exception_hook=True):
        """Create HTTP 1.1 keep-alive connection to the server with optional authorization details.

        :param add_exception_hook: Automatically raise an error in the case of HTTP error
        """

        session = requests.Session()

        if api_key:
            session.headers.update({'Authorization': api_key})

        if add_exception_hook:
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

    def get_cached_item(self, fname: Union[str, pathlib.Path]) -> Optional[pathlib.Path]:

        path = self.get_cached_file_path(fname)
        if not os.path.exists(path):
            return None

        f = pathlib.Path(path)

        end_time_pattern = r"-to_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
        if re.search(end_time_pattern, str(fname)):
            # Candle files with an end time never expire, as the history does not change
            return f

        mtime = datetime.datetime.fromtimestamp(f.stat().st_mtime)
        if datetime.datetime.now() - mtime > self.cache_period:
            # File cache expired
            return None

        return f

    def _generate_cache_name(
        self,
        pair_ids: Set[id],
        time_bucket: TimeBucket,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        max_bytes: Optional[int] = None,
    ) -> str:
        """Generate the name of the file for holding cached candle data for ``pair_ids``.
        """
        # Meaningfully truncate the timestamp to align with the target time bucket.
        if end_time:
            candle_width = time_bucket.to_timedelta()
            trunc = {"second": 0}
            if candle_width >= datetime.timedelta(hours=1):
                trunc["minute"] = 0
            if candle_width >= datetime.timedelta(days=1):
                trunc["hour"] = 0
            end_time = end_time.replace(**trunc)

        # Create a compressed cache key for the filename,
        # as we have 256 char limit on fname lenghts
        full_cache_key = (
            f"{pair_ids}{time_bucket}{start_time}{end_time}{max_bytes}"
        )
        md5 = hashlib.md5(full_cache_key.encode("utf-8")).hexdigest()

        # If exists, include the end time info in filename for cache invalidation logic.
        end_part = end_time.strftime("-to_%Y-%m-%d_%H-%M-%S") if end_time else ""

        return f"candles-jsonl-{time_bucket.value}{end_part}-{md5}.parquet"

    def purge_cache(self, filename: Optional[Union[str, pathlib.Path]] = None):
        """Delete all cached files on the filesystem.

        :param filename:
            If given, remove only that specific file, otherwise clear all cached data.
        """
        target_path = self.cache_path if filename is None else filename

        logger.info("Purging caches at %s", target_path)
        try:
            if os.path.isdir(target_path):
                shutil.rmtree(target_path)
            else:
                os.remove(target_path)
        except FileNotFoundError as exc:
            logger.warning(
                f"Attempted to purge caches, but no such file or directory: {exc.filename}"
            )

    def save_response(self, fpath, api_path, params=None, human_readable_hint: Optional[str]=None):
        """Download a file to the cache and display a pretty progress bar while doing it.

        :param fpath:
            File system path where the download will be saved

        :param api_path:
            Which Trading Strategy backtesting API we call to download the dataset.

        :param params:
            HTTP request params, like the `Authorization` header

        :param human_readable_hint:
            The status text displayed on the progress bar what's being downloaded
        """
        os.makedirs(self.get_abs_cache_path(), exist_ok=True)
        url = f"{self.endpoint}/{api_path}"
        logger.debug("Saving %s to %s", url, fpath)
        # https://stackoverflow.com/a/14114741/315168
        self.download_func(self.requests, fpath, url, params, self.timeout, human_readable_hint)

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
        self.save_response(path, "pair-universe", human_readable_hint="Downloading trading pair dataset")
        return self.get_cached_item(fname)

    def fetch_exchange_universe(self) -> pathlib.Path:
        fname = "exchange-universe.json"
        cached = self.get_cached_item(fname)
        if cached:
            return cached

        # Download save the file
        path = self.get_cached_file_path(fname)
        self.save_response(path, "exchange-universe", human_readable_hint="Downloading exchange dataset")
        return self.get_cached_item(fname)

    def fetch_candles_all_time(self, bucket: TimeBucket) -> pathlib.Path:
        assert isinstance(bucket, TimeBucket)
        fname = f"candles-{bucket.value}.parquet"
        cached = self.get_cached_item(fname)
        if cached:
            return cached
        # Download save the file
        path = self.get_cached_file_path(fname)
        self.save_response(path, "candles-all", params={"bucket": bucket.value}, human_readable_hint=f"Downloading OHLCV data for {bucket.value} time bucket")
        return self.get_cached_item(path)

    def fetch_liquidity_all_time(self, bucket: TimeBucket) -> pathlib.Path:
        fname = f"liquidity-samples-{bucket.value}.parquet"
        cached = self.get_cached_item(fname)
        if cached:
            return cached
        # Download save the file
        path = self.get_cached_file_path(fname)
        self.save_response(path, "liquidity-all", params={"bucket": bucket.value}, human_readable_hint=f"Downloading liquidity data for {bucket.value} time bucket")
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

    def fetch_candles_by_pair_ids(
            self,
            pair_ids: Set[id],
            time_bucket: TimeBucket,
            start_time: Optional[datetime.datetime] = None,
            end_time: Optional[datetime.datetime] = None,
            max_bytes: Optional[int] = None,
            progress_bar_description: Optional[str] = None,
    ) -> pd.DataFrame:
        """Load particular set of the candles and cache the result.

        If there is no cached result, load using JSONL.

        More information in :py:mod:`tradingstrategy.transport.jsonl`.

        For the candles format see :py:mod:`tradingstrategy.candle`.

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
            Display on downlood progress bar

        :return:
            Candles dataframe
        """
        cache_fname = self._generate_cache_name(
            pair_ids, time_bucket, start_time, end_time, max_bytes
        )
        cached = self.get_cached_item(cache_fname)

        if cached:
            full_fname = self.get_cached_file_path(cache_fname)
            logger.debug("Using cached JSONL data file %s", full_fname)
            return pandas.read_parquet(cached)

        df: pd.DataFrame = load_candles_jsonl(
            self.requests,
            self.endpoint,
            pair_ids,
            time_bucket,
            start_time,
            end_time,
            max_bytes=max_bytes,
            progress_bar_description=progress_bar_description,
        )

        # Update cache
        path = self.get_cached_file_path(cache_fname)
        df.to_parquet(path)

        size = pathlib.Path(path).stat().st_size
        logger.debug(f"Wrote {cache_fname}, disk size is {size:,}b")

        return df
