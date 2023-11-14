"""Get candlestick price and volume data from Binance.
"""

import requests
import datetime
import pandas as pd

from tradingstrategy.timebucket import TimeBucket
from pathlib import Path


def get_binance_candlestick_data(
    symbol: str,
    time_bucket: TimeBucket,
    start_at: datetime.datetime,
    end_at: datetime.datetime,
):
    """Get candlestick price and volume data from Binance. If saved, use saved version, else create saved version.

    .. code-block:: python
        five_min_data = get_binance_candlestick_data("ETHUSDC", TimeBucket.m5, datetime.datetime(2021, 1, 1), datetime.datetime(2021, 4, 1))

    :param symbol:
        Trading pair symbol E.g. ETHUSDC

    :param interval:
        Can be one of `1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M`

    :param start_at:
        Start date of the data

    :param end_at:
        End date of the data

    :return:
        Pandas dataframe with the OHLCV data for the columns and datetimes as the index
    """

    # to include the end date, we need to add one day
    end_at = end_at + datetime.timedelta(days=1)

    try:
        df = pd.read_parquet(
            _get_parquet_path(symbol, time_bucket, start_at, end_at)
        )
        return df
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

    for i in range(0, len(timestamps) - 1, 2):
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
                    dates.append(datetime.datetime.fromtimestamp(item[0] / 1000))
                    open_prices.append(float(item[1]))
                    high_prices.append(float(item[2]))
                    low_prices.append(float(item[3]))
                    close_prices.append(float(item[4]))
                    volume.append(float(item[5]))
        else:
            print(f"Error fetching data between {start_timestamp} and {end_timestamp}")
            print(f"Response: {response.status_code} {response.text}")

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

    df.to_parquet(_get_parquet_path(symbol, time_bucket, start_at, end_at))

    return df

def _get_parquet_path(
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