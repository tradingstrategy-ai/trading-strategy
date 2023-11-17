"""Get candlestick price and volume data from Binance.
"""

import requests
import datetime
import pandas as pd
import numpy as np
import logging

from tradingstrategy.timebucket import TimeBucket
from pathlib import Path


logger = logging.getLogger(__name__)


def get_binance_candlestick_data(
    symbol: str,
    time_bucket: TimeBucket,
    start_at: datetime.datetime,
    end_at: datetime.datetime,
    force_redownload=False,
):
    """Get clean candlestick price and volume data from Binance. If saved, use saved version, else create saved version.

    Note, if you want to use this data in our framework, you will need to add informational columns to the dataframe and overwrite it. See code below.

    .. code-block:: python
        symbol = "ETHUSDT"
        df = get_binance_candlestick_data(symbol, TimeBucket.h1, datetime.datetime(2021, 1, 1), datetime.datetime(2021, 4, 1))
        df = add_informational_columns(df, pair, EXCHANGE_SLUG)
        path = get_parquet_path(symbol, TimeBucket.h1, datetime.datetime(2021, 1, 1), datetime.datetime(2021, 4, 1))
        df.to_parquet(path)

    :param symbol:
        Trading pair symbol E.g. ETHUSDC

    :param interval:
        Can be one of `1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M`

    :param start_at:
        Start date of the data

    :param end_at:
        End date of the data

    :param force_redownload:
        Force redownload of data from Binance and overwrite cached version

    :return:
        Pandas dataframe with the OHLCV data for the columns and datetimes as the index
    """

    # to include the end date, we need to add one day
    end_at = end_at + datetime.timedelta(days=1)

    if not force_redownload:
        try:
            return get_binance_data_parquet(symbol, time_bucket, start_at, end_at)
        except:
            pass

    params_str = f"symbol={symbol}&interval={time_bucket.value}"

    if start_at:
        assert (
            end_at
        ), "If you specify a start_at, you must also specify an end_at"
        assert isinstance(
            start_at, datetime.datetime
        ), "start_at must be a datetime.datetime object"
        assert isinstance(
            end_at, datetime.datetime
        ), "end_at must be a datetime.datetime object"
        start_timestamp = int(start_at.timestamp() * 1000)
        end_timestamp = int(end_at.timestamp() * 1000)

    # generate timestamps for each iteration
    dates = [start_at]
    current_date = start_at
    while current_date < end_at:
        if (end_at - current_date) / time_bucket.to_timedelta() > 999:
            dates.append((current_date + time_bucket.to_timedelta() * 999))
            current_date += time_bucket.to_timedelta() * 999
        else:
            dates.append(end_at)
            current_date = end_at

    timestamps = [int(date.timestamp() * 1000) for date in dates]
    open_prices, high_prices, low_prices, close_prices, volume, dates = (
        [],
        [],
        [],
        [],
        [],
        [],
    )

    for i in range(0, len(timestamps) - 1):
        start_timestamp = timestamps[i]
        end_timestamp = timestamps[i + 1]
        full_params_str = (
            f"{params_str}&startTime={start_timestamp}&endTime={end_timestamp}"
        )
        url = f"https://api.binance.com/api/v3/klines?{full_params_str}&limit=1000"
        response = requests.get(url)
        if response.status_code == 200:
            json_data = response.json()
            if len(json_data) > 0:
                for item in json_data:
                    date_time = datetime.datetime.fromtimestamp(item[0] / 1000)
                    dates.append(date_time)
                    open_prices.append(float(item[1]))
                    high_prices.append(float(item[2]))
                    low_prices.append(float(item[3]))
                    close_prices.append(float(item[4]))
                    volume.append(float(item[5]))
        else:
            logger.warn(f"Error fetching data between {start_timestamp} and {end_timestamp}. \nResponse: {response.status_code} {response.text} \nMake sure you are using valid pair symbol e.g. `ETHUSDC`, not just ETH")

    df = pd.DataFrame(
        {
            "open": open_prices,
            "high": high_prices,
            "low": low_prices,
            "close": close_prices,
            "volume": volume,
        },
        index=dates,
    )

    # remove rows with index (date) duplicates
    df = df[df.index.duplicated(keep="first") == False]

    # df = clean_time_series_data(df)  
    path = get_parquet_path(symbol, time_bucket, start_at, end_at)
    df.to_parquet(path)

    return df


def get_binance_data_parquet(
    symbol: str,
    time_bucket: TimeBucket,
    start_at: datetime.datetime,
    end_at: datetime.datetime,
) -> pd.DataFrame:
    """Get parquet file for the candlestick data.

    :param symbol: Trading pair symbol E.g. ETHUSDC
    :param time_bucket: TimeBucket instance
    :param start_at: Start date of the data
    :param end_at: End date of the data
    :return: Path to the parquet file
    """
    try:
        return pd.read_parquet(
            get_parquet_path(symbol, time_bucket, start_at, end_at)
        )
    except:
        raise ValueError(
            "Parquet file not found. Use get_binance_candlestick_data to create it."
        )


def get_parquet_path(
    symbol: str,
    time_bucket: TimeBucket,
    start_at: datetime.datetime,
    end_at: datetime.datetime,
) -> Path:
    """Get parquet path for the candlestick data.
    
    :param symbol: Trading pair symbol E.g. ETHUSDC
    :param time_bucket: TimeBucket instance
    :param start_at: Start date of the data
    :param end_at: End date of the data
    :return: Path to the parquet file
    """
    return Path(
        f"./{symbol}-{time_bucket.value}-{start_at}-{end_at}.parquet"
    )


def clean_time_series_data(df: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
    """Unused for now since data from Binance occasionally has gaps. Not a huge deal.

    Cleans time series data to ensure:
    - No Nan values 
    - Index contains no duplicates
    - Has equally spaced intervals with no gaps
    - Sorted index in ascending order by datetime
    
    :param df: Pandas dataframe or series
    :return: Cleaned dataframe or series
    """

    if df.isna().any(axis=None):
        raise ValueError("Dataframe contains NaN values")
    
    if df.duplicated().any():
        raise ValueError("Dataframe contains duplicate values")
    
    assert type(df.index) == pd.DatetimeIndex, "Index must be a DatetimeIndex"

    df.sort_index(inplace=True)
    
    if len(uneven_indices := get_indices_of_uneven_intervals(df)) > 0:
        raise ValueError(f"Dataframe contains uneven intervals at indices {uneven_indices}")

    return df


def get_indices_of_uneven_intervals(df: pd.DataFrame | pd.Series) -> bool:
    """Checks if a time series contains perfectly evenly spaced time intervals with no gaps.
    
    :param df: Pandas dataframe or series
    :return: True if time series is perfectly evenly spaced, False otherwise
    """
    assert type(df.index) == pd.DatetimeIndex, "Index must be a DatetimeIndex"

    numeric_representation = df.index.astype(np.int64)

    differences = np.diff(numeric_representation)

    not_equal_to_first = differences != differences[0]

    return np.where(not_equal_to_first)[0]
