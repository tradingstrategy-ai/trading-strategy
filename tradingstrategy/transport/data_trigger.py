import datetime
from typing import Set

from requests import HTTPError, Timeout, TooManyRedirects

from tradingstrategy.client import Client
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.types import PrimaryKey

import abc
import datetime
import time
from dataclasses import dataclass
import logging
from typing import Set

from tradingstrategy.client import Client
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.types import PrimaryKey


logger = logging.getLogger(__name__)


#: Errors that are likely caused by flaky internet connection
#: and we should retry
RETRYABLE_EXCEPTIONS = (ConnectionError, HTTPError, Timeout, TooManyRedirects)


def wait_for_data(client: Client,
                  pair_ids: Set[PrimaryKey],
                  tick: datetime.datetime,
                  bucket: TimeBucket,
                  maximum_wait: datetime.timedelta,
                  sleep: datetime.timedelta = datetime.timedelta(seconds=5),
                  retryable_exceptions=RETRYABLE_EXCEPTIONS,
                  ):
    """Wait until the candle data is available.

    We wait until we have the data for the previous candle
    available.
    """

    logger.info("Waiting for strategy data for tick: %s, candle: %s")

    deadline = datetime.datetime.utcnow() + maximum_wait

    # Query only the latest candle from JSONL endpoint
    candle_ts = tick - bucket.to_pandas_timedelta()
    start_ts = candle_ts - datetime.timedelta(seconds=1)
    end_ts = candle_ts

    attempts = 1

    while datetime.datetime.utcnow() < deadline:
        logger.info("Starting to check the data age, attempt #%d", attempts)

        try:
            candles = client.fetch_candles_by_pair_ids(
                pair_ids,
                bucket,
                start_ts,
                end_ts,
            )

        except Exception as e:
            if not isinstance(e, retryable_exceptions):
                raise RuntimeError(f"Data wait fetch loop aborted") from e

        time.sleep(sleep.total_seconds())

        attempts += 1
