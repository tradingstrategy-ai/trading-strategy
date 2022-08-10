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

from tradingstrategy.candle import Candle
from tradingstrategy.timebucket import TimeBucket


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
) -> pd.DataFrame:
    """Read data from JSONL endpoint.

    `See OpenAPI spec for details on the format <https://tradingstrategy.ai/api/explorer/>`_.

    Can be used to load

    - OHLCV candles

    - Liquidity data

    Calling this function may consume up to few hundred megabytes
    of memory depending on the response size.

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

    # Massage the format good for pandas
    for idx, item in enumerate(reader):
        if "error" in item:
            raise JSONLMaxResponseSizeExceeded(str(item))

        # Translate the raw compressed keys to our internal
        # Pandas keys
        for key, value in item.items():
            translated_key = mappings[key]
            if translated_key is None:
                # Deprecated/discarded keys
                continue

            candle_data[translated_key].append(value)

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
    sanity_check_count=20,
) -> pd.DataFrame:
    """Load candles using JSON API and produce a DataFrame.

    Serially load each pair data.

    - Load data from per-pair JSON endpoint
    - Each pair becomes pandas :py:class:`pd.Series`
    - The final DataFrame is the merge of these series

    See :py:mod:`tradingstrategy.candle` for candle format description.

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
    )

    # Not supported at the momemnt
    df.loc[:, "avg"] = NaN

    df = df.astype(Candle.DATAFRAME_FIELDS)

    # Reconstruct normal volume column
    # as expected for OHLCV data
    df["volume"] = df["buy_volume"] + df["sell_volume"]

    df["timestamp"] = pd.to_datetime(df['timestamp'], unit='s')

    return df
