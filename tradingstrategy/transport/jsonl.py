"""Load candle and liquidity data over Trading Strategy real-time API.

[Use JSONL transport](https://tradingstrategy.ai/api/explorer/).
This method does not require API key at the moment.
"""
import logging
from collections import defaultdict

import pandas as pd

import datetime
from typing import List, Optional, Dict, Set

import requests
import jsonlines
from numpy import NaN
from tqdm.auto import tqdm

from tradingstrategy.candle import Candle
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.utils.time import to_int_unix_timestamp

logger = logging.getLogger(__name__)


#: Column name mappings from JSONL data to our :py:class:`~tradingstrategy.candle.Candle`.
CANDLE_MAPPINGS = {
    "ci": None,  # chain_id discarded
    "ei": None,  # exchange_id discarded
    "p": "pair_id",
    "ts": "timestamp",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "xr": "exchange_rate",
    "b": "buys",
    "s": "sells",
    "bv": "buy_volume",
    "sv": "sell_volume",
    "sb": "start_block",
    "eb": "end_block",
    "v": None,  # Deprecated
}


class JSONLMaxResponseSizeExceeded(Exception):
    """Raised if we ask too much JSONL data from the server."""


class NoJSONLData(Exception):
    """Server did not return any data for some reason."""


def load_trading_strategy_like_jsonl_data(
    session: requests.Session,
    api_url: str,
    pair_ids: Set[int],
    time_bucket: TimeBucket,
    mappings: Dict[str, str],
    start_time: Optional[datetime.datetime] = None,
    end_time: Optional[datetime.datetime] = None,
    max_bytes: Optional[int] = None,
    progress_bar_description: Optional[str] = None,
) -> pd.DataFrame:
    """Read data from JSONL endpoint.

    `See OpenAPI spec for details on the format <https://tradingstrategy.ai/api/explorer/>`_.

    Can be used to load

    - OHLCV candles

    - Liquidity data

    Calling this function may consume up to few hundred megabytes
    of memory depending on the response size.

    Display a progress bar using :py:mod:`tqdm`.

    :param df:
        The master DataFrame we are going to fill up.

    :param api_url:
        Which Trading Strategy API we call

    :param pair_ids:
        Trading pairs we query data for

    :param time_bucket:
        Candle time frame

    :param mappings:
        Mapping between JSONL object keys and DataFrame columns

    :return:
        In-place modified DataFrame passed to this function
    """

    params = {
        "pair_ids": ",".join(str(i) for i in pair_ids),
        "time_bucket": time_bucket.value,
    }

    if start_time:
        params["start_time"] = start_time.isoformat()

    if end_time:
        params["end_time"] = end_time.isoformat()

    if max_bytes:
        params["max_bytes"] = max_bytes

    logger.info("Loading JSON data, endpoint:%s, params:%s", api_url, params)

    resp = session.get(api_url, params=params, stream=True)
    reader = jsonlines.Reader(resp.raw)

    candle_data = defaultdict(list)

    # Figure out how to plot candle download progress using TQDM
    # Draw progress bar using timestamps first candle - last candle
    progress_bar_start = None
    progress_bar_end = end_time or datetime.datetime.utcnow()
    progress_bar_end = to_int_unix_timestamp(progress_bar_end)
    current_ts = last_ts = None
    progress_bar = None
    refresh_rate = 200  # Update the progress bar for every N candles

    # Massage the format good for pandas
    for idx, item in enumerate(reader):

        # Stream terminated
        if "error" in item:
            raise JSONLMaxResponseSizeExceeded(str(item))

        current_ts = item["ts"]

        # Set progress bar start to the first timestamp
        if not progress_bar_start and progress_bar_description:
            progress_bar_start = current_ts
            logger.debug("First candle timestamp at %s", current_ts)
            total = progress_bar_end - progress_bar_start
            assert progress_bar_start <= progress_bar_end, f"Mad progress bar {progress_bar_start} - {progress_bar_end}"
            progress_bar = tqdm(desc=progress_bar_description, total=total)

        # Translate the raw compressed keys to our internal
        # Pandas keys
        for key, value in item.items():
            translated_key = mappings[key]
            if translated_key is None:
                # Deprecated/discarded keys
                continue

            candle_data[translated_key].append(value)

        if idx % refresh_rate == 0:
            if last_ts and progress_bar:
                progress_bar.update(current_ts - last_ts)
                progress_bar.set_postfix({"Currently at": datetime.datetime.utcfromtimestamp(current_ts)})
            last_ts = current_ts

    if progress_bar:
        # https://stackoverflow.com/a/45808255/315168
        if current_ts and last_ts:
            progress_bar.update(current_ts - last_ts)
        progress_bar.close()

    if len(candle_data) == 0:
        raise NoJSONLData(f"Did not get any data, url:{api_url}, params:{params}")

    df = pd.DataFrame.from_dict(candle_data)

    logger.debug("Loaded %d rows", len(df))
    return df


def load_candles_jsonl(
    session: requests.Session,
    server_url: str,
    pair_ids: Set[id],
    time_bucket: TimeBucket,
    start_time: Optional[datetime.datetime] = None,
    end_time: Optional[datetime.datetime] = None,
    max_bytes: Optional[int] = None,
    progress_bar_description: Optional[str] = None,
    sanity_check_count=20,
) -> pd.DataFrame:
    """Load candles using JSON API and produce a DataFrame.

    Serially load each pair data.

    - Load data from per-pair JSON endpoint
    - Each pair becomes pandas :py:class:`pd.Series`
    - The final DataFrame is the merge of these series

    See :py:mod:`tradingstrategy.candle` for candle format description.

    :param progress_bar_description:
        Progress bar label

    :raise JSONLMaxResponseSizeExceeded:
        If the max_bytes limit is breached

    :return:
        Dataframe with candle data for giving pairs.
    """

    assert len(pair_ids) < sanity_check_count, f"We have {len(pair_ids)}, but you probably don't want to use this data fetch method for more than {sanity_check_count} pairs"

    api_url = f"{server_url}/candles-jsonl"

    df = load_trading_strategy_like_jsonl_data(
        session,
        api_url,
        pair_ids,
        time_bucket,
        CANDLE_MAPPINGS,
        start_time,
        end_time,
        max_bytes,
        progress_bar_description,
    )

    # Not supported at the momemnt
    df.loc[:, "avg"] = NaN

    df = df.astype(Candle.DATAFRAME_FIELDS)

    # Reconstruct normal volume column
    # as expected for OHLCV data
    df["volume"] = df["buy_volume"] + df["sell_volume"]

    # Convert JSONL unix timestamps to Pandas
    df["timestamp"] = pd.to_datetime(df['timestamp'], unit='s')

    # Assume candles are always indexed by their timestamp
    df.set_index("timestamp", inplace=True, drop=False)

    return df
