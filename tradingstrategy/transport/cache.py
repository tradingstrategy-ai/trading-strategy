"""A HTTP API transport that offers optional local caching of the results."""

import datetime
import enum
import hashlib
import json
import os
import pathlib
import platform
import re
import time
from contextlib import contextmanager
from http.client import IncompleteRead
from importlib.metadata import version
from json import JSONDecodeError
from pprint import pformat
from typing import Optional, Callable, Union, Collection, Dict, Tuple, Literal
import shutil
import logging
from pathlib import Path

from orjson import orjson
from requests.exceptions import ChunkedEncodingError
from urllib3 import Retry

import pandas
import pandas as pd
import requests
from filelock import FileLock
from requests import Response
from requests.adapters import HTTPAdapter
import pyarrow as pa

from tradingstrategy.candle import TradingPairDataAvailability
from tradingstrategy.chain import ChainId
from tradingstrategy.liquidity import XYLiquidity
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.token_metadata import TokenMetadata
from tradingstrategy.transport.jsonl import load_candles_jsonl, load_token_metadata_jsonl
from tradingstrategy.types import PrimaryKey, USDollarAmount, AnyTimestamp
from tradingstrategy.lending import LendingCandle, LendingCandleType
from tradingstrategy.transport.progress_enabled_download import download_with_tqdm_progress_bar


from tqdm_loggable.auto import tqdm

from tradingstrategy.utils.logging_retry import LoggingRetry

logger = logging.getLogger(__name__)


class OHLCVCandleType(enum.Enum):
    """Candle types for /candles endpoint

    See /candles as https://tradingstrategy.ai/api/explorer/#/Trading%20pair/web_candles
    """

    #: OHLCV price data
    price = "price"

    #: Contains one-sided liquidity for XYLiquidity Uniswap v2 pairs and some V3 dollar nominated pairs
    #:
    #: This is the legacy method.
    #:
    tvl_v1 = "tvl"

    #: Contains one-sided quote-token measured, dollar-nominated, TVL for Uniswap v2 and v3 pairs
    #:
    #: This is the recommended method.
    #:
    tvl_v2 = "tvl2"



class APIError(Exception):
    """API error parent class."""


class DataNotAvailable(APIError):
    """Data not available.

    This may happen e.g. when a new entry has just come online,
    it has been added to the pair or reserve map, but does not have candles available yet.

    Wraps 404 error from the dataset server.local
    """


class CacheStatus(enum.Enum):
    """When reading cached files, report to the caller about the caching status."""
    cached = "cached"
    cached_with_timestamped_name = "cached_with_timestamped_name"
    missing = "missing"
    expired = "expired"

    def is_readable(self):
        return self in (CacheStatus.cached, CacheStatus.cached_with_timestamped_name)


class CachedHTTPTransport:
    """A HTTP API transport that offers optional local caching of the results.

    - Download live and cached datasets from the candle server and cache locally
      on the filesystem

    - The download files are very large and expect to need several gigabytes of space for them

    - Has a default HTTP retry policy in the case network or server flakiness

    """

    def __init__(
        self,
         download_func: Callable,
         endpoint: Optional[str] = None,
         cache_period =datetime.timedelta(days=3),
         cache_path: Optional[str] = None,
         api_key: Optional[str] = None,
         timeout: float | tuple = (89.0, 89.0),
         add_exception_hook=True,
         retry_policy: Optional[Retry] = None
    ):
        """
        :param download_func: Interactive download progress bar displayed during the download
        :param endpoint: API server we are using - default is `https://tradingstrategy.ai/api`
        :param cache_period: How many days we store the downloaded files
        :param cache_path: Where we store the downloaded files
        :param api_key: Trading Strategy API key to use download
        :param timeout:
            Requests HTTP lib timeout.

            Passed to ``requests.get()``.

        :param add_exception_hook: Automatically raise an error in the case of HTTP error. Prevents auto retries.
        :param retry_policy:

            How to handle failed HTTP requests.
            If not given use the default somewhat graceful retry policy.
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
            self.cache_path = os.path.expanduser("~/.cache/tradingstrategy")

        self.requests = self.create_requests_client(
            api_key=api_key,
            retry_policy=retry_policy,
            add_exception_hook=add_exception_hook,
        )

        self.api_key = api_key
        self.timeout = timeout

    def close(self):
        """Release any underlying sockets."""
        self.requests.close()

    def create_requests_client(
        self,
        retry_policy: Optional[Retry]=None,
        api_key: Optional[str] = None,
        add_exception_hook=True,
    ):
        """Create HTTP 1.1 keep-alive connection to the server with optional authorization details.

        :param retry_policy:
            Override default retry policy.

        :param add_exception_hook: Automatically raise an error in the case of HTTP error
        """

        session = requests.Session()

        # Set up dealing with network connectivity flakey
        if retry_policy is None:
            # https://stackoverflow.com/a/35504626/315168
            retry_policy = LoggingRetry(
                total=5,
                backoff_factor=0.1,
                status_forcelist=[ 500, 502, 503, 504 ],
            )
            session.mount('http://', HTTPAdapter(max_retries=retry_policy))
            session.mount('https://', HTTPAdapter(max_retries=retry_policy))

        if api_key:
            session.headers.update({'Authorization': api_key})
            assert api_key.startswith("secret-token:"), f"API key must start with secret-token: - we got: {api_key[0:8]}..."

        # - Add default HTTP request retry policy to the client
        package_version = version("trading-strategy")
        system = platform.system()
        release = platform.release()
        session.headers.update({"User-Agent": f"trading-strategy {package_version} on {system} {release}"})

        if add_exception_hook:
            def exception_hook(response: Response, *args, **kwargs):
                if response.status_code == 404:
                    raise DataNotAvailable(f"Server error reply: code:{response.status_code} message:{response.text}")
                elif response.status_code >= 400:
                    raise APIError(f"Server error reply: code:{response.status_code} message:{response.text}")

            session.hooks = {
                "response": exception_hook,
            }
        return session

    def get_abs_cache_path(self) -> Path:
        return Path(os.path.abspath(self.cache_path))

    def get_cached_file_path(self, fname):
        path = os.path.join(self.get_abs_cache_path(), fname)
        return path

    def get_cached_item(self, fname: Union[str, pathlib.Path]) -> Optional[pathlib.Path]:
        """Get a cached file.

        - Return ``None`` if the cache has expired

        - The cache timeout is coded in the file modified
          timestamp (mtime)
        """

        path = self.get_cached_file_path(fname)
        if not os.path.exists(path):
            # Cached item not yet created
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

    def get_cached_item_with_status(
            self,
            fname: Union[str, pathlib.Path]
    ) -> Tuple[pathlib.Path | None, CacheStatus]:
        """Get a cached file.

        - Return ``None`` if the cache has expired

        - The cache timeout is coded in the file modified
          timestamp (mtime)
        """

        path = self.get_cached_file_path(fname)
        if not os.path.exists(path):
            # Cached item not yet created
            return None, CacheStatus.missing

        f = pathlib.Path(path)

        # For some datasets, we encode the end-tie in the fname
        end_time_pattern = r"-to_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
        if re.search(end_time_pattern, str(fname)):
            # Candle files with an end time never expire, as the history does not change
            return f, CacheStatus.cached_with_timestamped_name

        mtime = datetime.datetime.fromtimestamp(f.stat().st_mtime)
        if datetime.datetime.now() - mtime > self.cache_period:
            # File cache expired
            return None, CacheStatus.expired

        return f, CacheStatus.cached

    def _generate_cache_name(
        self,
        pair_ids: Collection[PrimaryKey] | PrimaryKey,
        time_bucket: TimeBucket,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        max_bytes: Optional[int] = None,
        candle_type: str = "candles",
        ftype: Literal["parquet", "jsonl"]="parquet",
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

        if type(pair_ids) is int:
            # Backwards compat
            pass
        else:
            pair_ids = sorted(list(pair_ids))

        # Create a compressed cache key for the filename,
        # as we have 256 char limit on fname lenghts
        full_cache_key = (
            f"{candle_type}{ftype}{pair_ids}{time_bucket}{start_time}{end_time}{max_bytes}"
        )
        md5 = hashlib.md5(full_cache_key.encode("utf-8")).hexdigest()

        # If exists, include the end time info in filename for cache invalidation logic.
        if start_time:
            start_part = start_time.strftime("%Y-%m-%d_%H-%M-%S")
        else:
            start_part = "any"

        end_part = end_time.strftime("%Y-%m-%d_%H-%M-%S") if end_time else "any"

        return f"{candle_type.replace('_', '-')}-{time_bucket.value}-between-{start_part}-and-{end_part}-{md5}.{ftype}"

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

    def get_json_response(self, api_path, params=None, attempts=5, sleep=30.0) -> dict:
        url = f"{self.endpoint}/{api_path}"
        logger.debug("get_json_response() %s, %s", url, params)

        # TODO: Replace custom retryable with a generic Requests library retry handler

        # Because we can also fail in decoding HTTP response, not just HTTP error code,
        # we need a special logic here
        # requests.exceptions.ChunkedEncodingError: Response ended  prematurely
        retryable = (ChunkedEncodingError,)

        response: Response
        for attempt in range(attempts):
            try:
                response = self.requests.get(
                    url,
                    params=params,
                    timeout=self.timeout,
                )

                if not (200 <= response.status_code <= 299):
                    logger.warning(
                        "Attempt #%d, received code %d: %s",
                        attempt + 1,
                        response.status_code,
                        response.text[0:300],
                    )
                    time.sleep(sleep)
                    continue

                break

            except Exception as e:
                if isinstance(e, retryable):
                    logger.warning("Attempt #%d, received %s", attempt + 1, e)
                    time.sleep(sleep)
                    continue
                raise

        if not (200 <= response.status_code <= 299):
            raise APIError(f"Could not call {url}\nParams: {params}\nResponse: {response.status_code} {response.text}")
        return response.json()

    def post_json_response(self, api_path, params=None):
        url = f"{self.endpoint}/{api_path}"
        response = self.requests.post(url, params=params)
        return response.json()

    def fetch_chain_status(self, chain_id: int) -> dict:
        """Not cached."""
        return self.get_json_response("chain-status", params={"chain_id": chain_id})

    def fetch_top_pairs(
        self,
        chain_ids: Collection[ChainId],
        exchange_slugs: Collection[str],
        addresses: Collection[str],
        method: str,
        min_volume_24h_usd: None | USDollarAmount = None,
        limit: int | None = None,
        risk_score_threshold: int | None = None,
    ) -> dict:
        """Call /top API endpoint.

        - Not cached.
        """
        params = {
            "chain_slugs": ",".join([c.get_slug() for c in chain_ids]),
            "method": method,
        }

        # OpenAPI list syntax
        if exchange_slugs:
            params["exchange_slugs"] = ",".join([e for e in exchange_slugs])

        if addresses is not None:
            params["addresses"] = ",".join([a for a in addresses])

        if limit is not None:
            params["limit"] = str(limit)

        if min_volume_24h_usd is not None:
            params["min_volume_24h_usd"] = str(min_volume_24h_usd)

        if risk_score_threshold is not None:
            params["risk_score_threshold"] = str(risk_score_threshold)

        logger.info(
            "/top call with params %s",
            pformat(params),
        )

        resp = self.get_json_response("top", params=params)
        return resp

    def fetch_pair_universe(self) -> pathlib.Path:
        fname = "pair-universe.parquet"
        cached = self.get_cached_item(fname)

        # Download save the file
        path = self.get_cached_file_path(fname)

        with wait_other_writers(path):

            if cached:
                logger.info("Using cached pair universe %s", path)
                return cached

            logger.info("Downloading pair universe data from the server %s", path)
            self.save_response(path, "pair-universe", human_readable_hint="Downloading trading pair dataset")

            # TODO: Some sort of race consition can be here?
            path = self.get_cached_item(fname)
            assert path is not None, f"Failed to get cached item {fname} in fetch_pair_universe()"
            return path

    def fetch_exchange_universe(self) -> pathlib.Path:
        fname = "exchange-universe.json"

        # Download save the file
        path = self.get_cached_file_path(fname)

        with wait_other_writers(path):

            cached = self.get_cached_item(fname)
            if cached:
                return cached

            self.save_response(path, "exchange-universe", human_readable_hint="Downloading exchange dataset")

            _check_good_json(path, "fetch_exchange_universe() failed")

            return self.get_cached_item(fname)
    
    def fetch_lending_reserve_universe(self) -> pathlib.Path:
        fname = "lending-reserve-universe.json"
        cached = self.get_cached_item(fname)

        # Download save the file
        path = self.get_cached_file_path(fname)

        with wait_other_writers(path):

            if cached:
                return cached

            self.save_response(
                path,
                "lending-reserve-universe",
                human_readable_hint="Downloading lending reserve dataset"
            )
            path, status = self.get_cached_item_with_status(fname)

            _check_good_json(path, "fetch_lending_reserve_universe() failed")

            assert status.is_readable(), f"Got status {status} for path"
            return path

    def fetch_candles_all_time(self, bucket: TimeBucket) -> pathlib.Path:
        """Load candles and return a cached file where they are stored.

        - If cached file exists return it directly

        - Wait if someone else is writing the file
          (in multiple parallel testers)
        """
        assert isinstance(bucket, TimeBucket)
        fname = f"candles-{bucket.value}.parquet"
        cached_path = self.get_cached_file_path(fname)

        with wait_other_writers(cached_path):

            cached = self.get_cached_item(fname)
            if cached:
                # Cache exists and is not expired
                return cached

            # Download save the file
            params = {"bucket": bucket.value}
            self.save_response(cached_path, "candles-all", params, human_readable_hint=f"Downloading OHLCV data for {bucket.value} time bucket")
            logger.info(
                "Saved %s as with params %s, down",
                cached_path,
                params
            )
            saved, status = self.get_cached_item_with_status(fname)
            # Troubleshoot multiple test workers race condition
            assert status.is_readable(), f"Cache status {status} with save_response() generated for {fname}, cached path is {cached_path}, download_func is {self.download_func}"
            return saved

    def fetch_liquidity_all_time(self, bucket: TimeBucket) -> pathlib.Path:
        fname = f"liquidity-samples-{bucket.value}.parquet"
        path = self.get_cached_file_path(fname)

        with wait_other_writers(path):

            cached = self.get_cached_item(fname)
            if cached:
                return cached
            # Download save the file
            self.save_response(path, "liquidity-all", params={"bucket": bucket.value}, human_readable_hint=f"Downloading liquidity data for {bucket.value} time bucket")
            return self.get_cached_item(path)

    def fetch_lending_reserves_all_time(self) -> pathlib.Path:
        fname = "lending-reserves-all.parquet"

        # Download save the file
        path = self.get_cached_file_path(fname)

        with wait_other_writers(path):

            cached = self.get_cached_item(fname)
            if cached:
                return cached

            # We only have Aave v3 data for now...
            self.save_response(
                path,
                "aave-v3-all",
                human_readable_hint="Downloading Aave v3 reserve dataset",
            )
            assert os.path.exists(path)
            item, status = self.get_cached_item_with_status(path)
            assert status.is_readable(), f"File not readable after save cached:{cached} fname:{fname} path:{path}"
            return item
    
    def fetch_lending_candles_by_reserve_id(
        self,
        reserve_id: int,
        time_bucket: TimeBucket,
        candle_type: LendingCandleType = LendingCandleType.variable_borrow_apr,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
    ) -> pd.DataFrame:
        """Load particular set of the lending candles and cache the result.

        For the candles format see :py:mod:`tradingstrategy.lending`.

        :param reserve_id:
            Lending reserve's internal id we query data for.
            Get internal id from lending reserve universe dataset.

        :param time_bucket:
            Candle time frame.

        :param candle_type:
            Lending candle type.

        :param start_time:
            All candles after this.
            If not given start from genesis.

        :param end_time:
            All candles before this

        :return:
            Lending candles dataframe
        """

        assert  isinstance(time_bucket, TimeBucket)
        assert isinstance(candle_type, LendingCandleType)

        cache_fname = self._generate_cache_name(
            reserve_id,
            time_bucket,
            start_time,
            end_time,
            candle_type=candle_type.name,
        )

        full_fname = self.get_cached_file_path(cache_fname)

        with wait_other_writers(full_fname):

            cached = self.get_cached_item(cache_fname)

            if cached:
                logger.debug("Using cached data file %s", full_fname)
                return pandas.read_parquet(cached)

            api_url = f"{self.endpoint}/lending-reserve/candles"

            params = {
                "reserve_id": reserve_id,
                "time_bucket": time_bucket.value,
                "candle_types": candle_type,
            }

            if start_time:
                params["start"] = start_time.isoformat()

            if end_time:
                params["end"] = end_time.isoformat()

            try:
                resp = self.requests.get(api_url, params=params, stream=True)
            except DataNotAvailable as e:
                # We have special request hook that translates 404 to this exception
                raise DataNotAvailable(f"Could not fetch lending candles for {params}") from e
            except Exception as e:
                raise APIError(f"Could not fetch lending candles for {params}") from e

            # TODO: handle error
            candles = resp.json()[candle_type]

            df = LendingCandle.convert_web_candles_to_dataframe(candles)

            # Update cache
            path = self.get_cached_file_path(cache_fname)
            df.to_parquet(path)

            size = pathlib.Path(path).stat().st_size
            logger.debug(f"Wrote {cache_fname}, disk size is {size:,}b")

            return df

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
        pair_ids: Collection[PrimaryKey],
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

        full_fname = self.get_cached_file_path(cache_fname)

        with wait_other_writers(full_fname):

            cached = self.get_cached_item(cache_fname)

            if cached:
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
                # temp increase sanity check count
                sanity_check_count=1_500,
            )

            # Update cache
            path = self.get_cached_file_path(cache_fname)
            df.to_parquet(path)

            size = pathlib.Path(path).stat().st_size
            logger.debug(f"Wrote {cache_fname}, disk size is {size:,}b")

            return df

    def _fetch_tvl_by_pair_id(
        self,
        pair_id: PrimaryKey,
        time_bucket: TimeBucket,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        query_type: OHLCVCandleType = OHLCVCandleType.tvl_v1,
    ) -> pd.DataFrame:
        """Internal hack to load TVL data for a single pair.

        - See :py:meth:`fetch_tvl_by_pair_ids` for public API.

        - TODO: Currently there is no JSONL endpoint to get liquidity data streaming.
          Thus, we do this hack here. Also because of different data format for Uni v2 and Uni v3,
          the server cannot handle fetching mixed pool types once.

        - This function causes double caching and such.

        - Will be replaced by a proper JSONL streaming in somepoint.

        """

        assert query_type in (OHLCVCandleType.tvl_v1, OHLCVCandleType.tvl_v2,), f"Got: {query_type}"

        pair_ids = [pair_id]

        candle_type = query_type.value

        cache_fname = self._generate_cache_name(
            pair_ids,
            time_bucket,
            start_time,
            end_time,
            candle_type=candle_type,
        )

        full_fname = self.get_cached_file_path(cache_fname)

        with wait_other_writers(full_fname):

            cached = self.get_cached_item(cache_fname)
            path = self.get_cached_file_path(cache_fname)

            if cached:
                # We have a locally cached version
                logger.debug("Using cached Parquet data file %s", full_fname)
                df = pandas.read_parquet(cached)
            else:
                # Read from the server, store in the disk
                params = {
                    "time_bucket": time_bucket.value,
                    # "pair_ids": ",".join([str(i) for i in pair_ids]),  # OpenAPI comma delimited array
                    "pair_id": pair_ids[0],
                }

                params["candle_type"] = query_type.value

                if start_time:
                    params["start"] = start_time.isoformat()

                if end_time:
                    params["end"] = end_time.isoformat()

                # Use /candles endpoint to load TVL data
                pair_candle_map = self.get_json_response(
                    "candles",
                    params=params,
                )

                if len(pair_candle_map) > 0:
                    # This pair has valid data
                    for pair_id, array in pair_candle_map.items():
                        df = XYLiquidity.convert_web_candles_to_dataframe(array)
                        # Fill in pair id,
                        # because /candles endpoint does not reflect it back
                        df["pair_id"] = int(pair_id)

                        # Update cache - we store a single file per pair
                        # at the moment
                        df.to_parquet(path)

                else:
                    # This pair has TVL data missing on the server,
                    # because we asked for a single pair and did not get any data
                    logger.warning("Pair id %d - could not load TVL/liquidity data", pair_id)
                    df = XYLiquidity.convert_web_candles_to_dataframe([])
                    # Update cache - we store a single file per pair
                    # at the moment
                    df.to_parquet(path)

            size = pathlib.Path(path).stat().st_size
            if not cached:
                logger.debug(f"Wrote {cache_fname}, disk size is {size:,}b")
            else:
                logger.debug(f"Read {cache_fname}, disk size is {size:,}b")

            # Expose if this reply was cached or not,
            # for unit testing
            df.attrs["cached"] = cached
            df.attrs["disk_size"] = size

        return df

    def fetch_tvl_by_pair_ids(
        self,
        pair_ids: Collection[PrimaryKey],
        time_bucket: TimeBucket,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        progress_bar_description: Optional[str] = None,
        query_type: OHLCVCandleType = OHLCVCandleType.tvl_v1,
    ) -> pd.DataFrame:
        """Load particular set of the TVL candles and cache the result.

        For the candles format see :py:mod:`tradingstrategy.liquidity`.

        :param pair_ids:
            Trading pairs internal ids we query data for.
            Get internal ids from pair dataset.

            We should be able to handle unlimited pair count,
            as we do one request per pair.

        :param time_bucket:
            Candle time frame

        :param start_time:
            All candles after this.
            If not given start from genesis.

        :param end_time:
            All candles before this

        :param progress_bar_description:
            Display on downlood progress bar

        :return:
            Liquidity dataframe.

            See :py:mod:`tradingstrategy.liquidity`.
        """

        #
        # TODO: Currently there is no JSONL
        # endpoint to get liquidity data streaming.
        # Thus, we do this hack here.
        # Also because of different data format for Uni v2 and Uni v3,
        # the server cannot handle fetching mixed pool types
        # once.
        #

        chunks = []

        if progress_bar_description:
            # The server does not know the reply size,
            # so we cannot render a progress bar estimation
            progress_bar = tqdm(desc=progress_bar_description, total=len(pair_ids))
        else:
            progress_bar = None

        for pair_id in pair_ids:

            df = self._fetch_tvl_by_pair_id(
                pair_id,
                time_bucket,
                start_time,
                end_time,
                query_type=query_type,
            )

            assert "timestamp" in df.columns, f"Columns lack timestamp: {df.columns}"
            assert "pair_id" in df.columns, f"Columns lack pair_id: {df.columns}"

            # Work around pd.concat() problem for some reason fails on Github,
            # see details below
            df = df.set_index(["pair_id", "timestamp"], drop=False)

            chunks.append(df)

            if progress_bar:
                progress_bar.update()

        if progress_bar:
            progress_bar.close()

        try:
            parts = [c for c in chunks if len(c) > 0]
            if len(parts) > 0:
                return pd.concat(parts)
            else:
                return pd.DataFrame()
        except ValueError as e:
            # Happens only on Github CI
            # https://stackoverflow.com/questions/27719407/pandas-concat-valueerror-shape-of-passed-values-is-blah-indices-imply-blah2
            msg = ""
            for c in chunks:
                msg += f"Index: {c.index}\n"
                msg += f"Data: {c}\n"
            raise ValueError(f"pd.concat() failed:\n{msg}") from e

    def fetch_clmm_liquidity_provision_candles_by_pair_ids(
        self,
        pair_ids: Collection[PrimaryKey],
        time_bucket: TimeBucket,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        progress_bar_description: Optional[str] = None,
    ) -> pd.DataFrame:
        """Stream CLMM Parquet data from the server.

        For the candles format see :py:mod:`tradingstrategy.clmm`.

        :param pair_ids:
            Trading pairs internal ids we query data for.
            Get internal ids from pair dataset.

            We should be able to handle unlimited pair count,
            as we do one request per pair.

        :param time_bucket:
            Candle time frame

        :param start_time:
            All candles after this.
            If not given start from genesis.

        :param end_time:
            All candles before this

        :param progress_bar_description:
            Display on downlood progress bar

        :return:
            CLMM dataframe.

            See :py:mod:`tradingstrategy.clmm`.
        """

        cache_fname = self._generate_cache_name(
            pair_ids, time_bucket, start_time, end_time,
            candle_type="clmm"
        )

        full_fname = self.get_cached_file_path(cache_fname)

        url = f"{self.endpoint}/clmm-candles"

        with wait_other_writers(full_fname):

            cached = self.get_cached_item(cache_fname)
            path = self.get_cached_file_path(cache_fname)

            if not cached:

                params = {
                    "pair_ids": ",".join([str(i) for i in pair_ids]),  # OpenAPI comma delimited array
                    "time_bucket": time_bucket.value,
                    "format": "parquet",
                }

                if start_time:
                    params["start"] = start_time.isoformat()

                if end_time:
                    params["end"] = end_time.isoformat()

                download_with_tqdm_progress_bar(
                    session=self.requests,
                    path=path,
                    url=url,
                    params=params,
                    timeout=self.timeout,
                    human_readable_hint=progress_bar_description,
                )

                size = pathlib.Path(path).stat().st_size
                logger.debug(f"Wrote {cache_fname}, disk size is {size:,}b")

            else:
                size = pathlib.Path(path).stat().st_size
                logger.debug(f"Reading cached Parquet file {cache_fname}, disk size is {size:,}")

            df = pandas.read_parquet(path)

            # Export cache metadata
            df.attrs["cached"] = cached is not None
            df.attrs["filesize"] = size
            df.attrs["path"] = path
            return df

    def fetch_tvl(
        self,
        time_bucket: TimeBucket,
        mode: Literal["min_tvl", "min_tvl_low", "pair_ids"],
        exchange_ids: Collection[PrimaryKey] = None,
        pair_ids: Collection[PrimaryKey] = None,
        start_time: Optional[AnyTimestamp] = None,
        end_time: Optional[AnyTimestamp] = None,
        min_tvl: Optional[USDollarAmount] = None,
        progress_bar_description: Optional[str] = "Downloading TVL data",
        min_tvl_timeout=(240, 240),
        max_attempts=2,
    ) -> pd.DataFrame:
        """Stream TVL Parquet data from the server.

        :param timeout:
            We need to override the default timeout with longer one,
            because the min_tvl prefilter step is heavy.

        :return:
            TVL dataframe.

            See :py:mod:`tradingstrategy.tvl`.
        """

        assert isinstance(time_bucket, TimeBucket)

        match mode:
            case "pair_ids":
                assert pair_ids
                assert type(pair_ids) in (list, tuple, set)
                cache_fname = self._generate_cache_name(
                    pair_ids, time_bucket, start_time, end_time,
                    candle_type="tvl",
                    ftype="parquet",
                )
                timeout = self.timeout
            case "min_tvl" | "min_tvl_low":
                assert exchange_ids
                assert type(exchange_ids) in (list, tuple, set)
                assert type(min_tvl) in (float, int), f"min_tvl must be float, got {type(min_tvl)}: {min_tvl}"
                cache_fname = self._generate_cache_name(
                    exchange_ids, time_bucket, start_time, end_time,
                    candle_type=f"{mode.replace('_', '-')}-{min_tvl}",
                    ftype="parquet",
                )
                timeout = min_tvl_timeout
            case _:
                raise NotImplementedError(f"Unsupported mode: {mode}")

        full_fname = self.get_cached_file_path(cache_fname)

        url = f"{self.endpoint}/tvl"

        attempt = 0
        while attempt < max_attempts:
            with wait_other_writers(full_fname):

                cached = self.get_cached_item(cache_fname)
                path = self.get_cached_file_path(cache_fname)

                if not cached:

                    params = {
                        "time_bucket": time_bucket.value,
                        "mode": mode,
                    }

                    if pair_ids:
                        params["pair_ids"] = ",".join([str(i) for i in pair_ids]),  # OpenAPI comma delimited array

                    if exchange_ids:
                        params["exchange_ids"] = ",".join([str(i) for i in exchange_ids]),  # OpenAPI comma delimited array

                    if start_time:
                        params["start"] = start_time.isoformat()

                    if end_time:
                        params["end"] = end_time.isoformat()

                    if min_tvl:
                        params["min_tvl"] = str(min_tvl)

                    logger.info(
                        "fetch_tvl(): no cache hit, timeout %s\nparams: %s\ncache path: %s",

                        timeout,
                        params,
                        path,
                    )

                    download_with_tqdm_progress_bar(
                        session=self.requests,
                        path=path,
                        url=url,
                        params=params,
                        timeout=timeout,
                        human_readable_hint=progress_bar_description,
                    )

                    size = pathlib.Path(path).stat().st_size
                    logger.debug(f"Wrote {cache_fname}, disk size is {size:,}b")

                else:
                    logger.info("fetch_tvl(): cache hit for %s", path)
                    size = pathlib.Path(path).stat().st_size
                    logger.debug(f"Reading cached Parquet file {cache_fname}, disk size is {size:,}")

                try:
                    df = pandas.read_parquet(path)
                    break
                except (pa.ArrowInvalid, IncompleteRead) as e:
                    attempt += 1
                    msg = f"Parquet file {path} with size {size:,} bytes invalid, cached is {cached}: {e}"
                    if attempt >= max_attempts:
                        raise RuntimeError(msg) from e
                    else:
                        logger.warning(msg)
                        pathlib.Path(path).unlink()
                        logger.warning("Cache cleared: %s", path)

        # Export cache metadata
        df.attrs["cached"] = cached is not None
        df.attrs["filesize"] = size
        df.attrs["path"] = path

        range_start = df["bucket"].min()
        range_end = df["bucket"].max()

        pair_count = len(df["pair_id"].unique())

        logger.info(
            "Got TVL data for range %s - %s (requested %s - %s), pair count: %d, candle count: %d",
            range_start,
            range_end,
            start_time,
            end_time,
            pair_count,
            len(df)
        )

        return df

    def fetch_trading_data_availability(self,
          pair_ids: Collection[PrimaryKey],
          time_bucket: TimeBucket,
        ) -> Dict[PrimaryKey, TradingPairDataAvailability]:
        """Check the trading data availability at oracle's real time market feed endpoint.

        - Trading Strategy oracle uses sparse data format where candles
          with zero trades are not generated. This is better suited
          for illiquid DEX markets with few trades.

        - Because of sparse data format, we do not know if there is a last
          candle available - candle may not be available yet or there might not be trades
          to generate a candle

        - This endpoint allows to check the trading data availability for multiple of trading pairs.

        - This endpoint is public

        :param pair_ids:
            Trading pairs internal ids we query data for.
            Get internal ids from pair dataset.

        :param time_bucket:
            Candle time frame

        :return:
            Map of pairs -> their trading data availability

        """

        params = {
            "pair_ids": ",".join([str(i) for i in pair_ids]),  # OpenAPI comma delimited array
            "time_bucket":  time_bucket.value,
        }

        array = self.get_json_response("trading-pair-data-availability", params=params)

        # Make to typed and deseralise
        def _convert(p: dict) -> TradingPairDataAvailability:
            try:
                return {
                    "chain_id": ChainId(p["chain_id"]),
                    "pair_id": p["pair_id"],
                    "pair_address": p["pair_address"],
                    "last_trade_at": datetime.datetime.fromisoformat(p["last_trade_at"]),
                    "last_candle_at": datetime.datetime.fromisoformat(p["last_candle_at"]),
                    "last_supposed_candle_at": datetime.datetime.fromisoformat(p["last_supposed_candle_at"]),
                }
            except Exception as e:
                raise RuntimeError(f"Failed to convert: {p}") from e

        return {p["pair_id"]: _convert(p) for p in array}

    def fetch_token_metadata(
        self,
        chain_id: ChainId,
        addresses: Collection[str],
        progress_bar_description: str | None,
    ) -> dict[str, TokenMetadata]:
        """Load cached token metadata

        - Cache on this, one JSON file per token

        - Only load token metadata for cached files we do not have
        """

        base_cache = Path(self.cache_path) / "token-metadata"
        os.makedirs(base_cache, exist_ok=True)

        def get_cache_path(address: str) -> Path:
            assert address.startswith("0x")
            return base_cache / f"{chain_id.value}-{address}.json"

        # Find metadata which we have already loaded
        addresses = set(a.lower() for a in addresses)
        cached = {a for a in addresses if get_cache_path(a).exists()}
        uncached = addresses - cached

        # Load items we have not locally
        if len(uncached) > 0:
            fresh_load = load_token_metadata_jsonl(
                session=self.requests,
                server_url=self.endpoint,
                chain_id=chain_id,
                addresses=uncached,
                progress_bar_description=progress_bar_description,
            )
        else:
            fresh_load = {}

        # Save cached
        for address, data in fresh_load.items():
            data["cached"] = False
            with open(get_cache_path(address), "wb") as f:
                f.write(orjson.dumps(data))

        # Load existing
        cached_load = {}
        for address in cached:
            with open(get_cache_path(address), "rb") as f:
                data = orjson.loads(f.read())
                data["cached"] = True
                cached_load[address] = data

        logger.info("Server-side loaded: %d, cache loaded: %d", len(fresh_load), len(cached_load))
        full_set = fresh_load | cached_load

        # Return and convert to TokenMetadata instances
        return {address: TokenMetadata(**item) for address, item in full_set.items()}


@contextmanager
def wait_other_writers(path: Path | str, timeout=120):
    """Wait other potential writers writing the same file.

    - Work around issues when parallel unit tests and such
      try to write the same file

    Example:

    .. code-block:: python

        import urllib
        import tempfile

        import pytest
        import pandas as pd

        @pytest.fixture()
        def my_cached_test_data_frame() -> pd.DataFrame:

            # Al tests use a cached dataset stored in the /tmp directory
            path = os.path.join(tempfile.gettempdir(), "my_shared_data.parquet")

            with wait_other_writers(path):

                # Read result from the previous writer
                if not path.exists(path):
                    # Download and write to cache
                    urllib.request.urlretrieve("https://example.com", path)

                return pd.read_parquet(path)

    :param path:
        File that is being written

    :param timeout:
        How many seconds wait to acquire the lock file.

        Default 2 minutes.

    :raise filelock.Timeout:
        If the file writer is stuck with the lock.
    """

    if type(path) == str:
        path = Path(path)

    assert isinstance(path, Path), f"Not Path object: {path}"

    assert path.is_absolute(), f"Did not get an absolute path: {path}\n" \
                               f"Please use absolute paths for lock files to prevent polluting the local working directory."

    # If we are writing to a new temp folder, create any parent paths
    os.makedirs(path.parent, exist_ok=True)

    # https://stackoverflow.com/a/60281933/315168
    lock_file = path.parent / (path.name + '.lock')

    lock = FileLock(lock_file, timeout=timeout)

    if lock.is_locked:
        logger.info(
            "Parquet file %s locked for writing, waiting %f seconds",
            path,
            timeout,
        )

    with lock:
        yield


def _check_good_json(path: Path, exception_message: str):
    """Check that server gave us good JSON file.

    - 404, 500, API key errors

    """
    broken_data = False

    # Quick fix to avoid getting hit by API key errors here.
    # TODO: Clean this up properly
    with open(path, "rt", encoding="utf-8") as inp:
        data = inp.read()
        try:
            data = json.loads(data)
            if "error" in data:
                broken_data = True
        except JSONDecodeError as e:
            broken_data = True

    if broken_data:
        os.remove(path)
        raise RuntimeError(f"{exception_message}\nJSON data is: {data}")

