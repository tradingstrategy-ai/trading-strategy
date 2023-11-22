"""Get candlestick price and volume data from Binance.
"""

import requests
import datetime
import pandas as pd
import numpy as np
import logging
import shutil

from tradingstrategy.timebucket import TimeBucket
from pathlib import Path
from tradingstrategy.utils.time import generate_monthly_timestamps
from tradingstrategy.utils.groupeduniverse import resample_series


logger = logging.getLogger(__name__)


class BinanceDownloader:
    """Class for downloading Binance candlestick OHLCV data."""

    def __init__(self, cache_directory: Path = Path("/tmp/binance_data")):
        """Initialize BinanceCandleDownloader and create folder for cached data if it does not exist."""
        cache_directory.mkdir(parents=True, exist_ok=True)
        self.cache_directory = cache_directory

    def fetch_candlestick_data(
        self,
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
        if not force_redownload:
            try:
                return self.get_data_parquet(symbol, time_bucket, start_at, end_at)
            except:
                pass

        # to include the end date, we need to add one day
        end_at = end_at + datetime.timedelta(days=1)
        df = self._fetch_candlestick_data(symbol, time_bucket, start_at, end_at)
        end_at = end_at - datetime.timedelta(days=1)

        # write to parquet
        path = self.get_parquet_path(symbol, time_bucket, start_at, end_at)
        df.to_parquet(path)

        return df

    def _fetch_candlestick_data(
        self,
        symbol: str,
        time_bucket: TimeBucket,
        start_at: datetime.datetime,
        end_at: datetime.datetime,
    ):
        params_str = f"symbol={symbol}&interval={time_bucket.value}"

        if start_at:
            assert end_at, "If you specify a start_at, you must also specify an end_at"
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
                raise ValueError(
                    f"Error fetching data between {start_timestamp} and {end_timestamp}. \nResponse: {response.status_code} {response.text} \nMake sure you are using valid pair symbol e.g. `ETHUSDC`, not just ETH"
                )

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

        # df = clean_time_series_data(df)

        # Each timestamp in `timestamps` besides the first and last entry will be duplicated, so remove
        remove_duplicates_df = df[df.index.duplicated(keep="first") == False]

        return remove_duplicates_df

    def fetch_lending_rates(
        self,
        asset_symbol: str,
        time_bucket: TimeBucket,
        start_at: datetime.datetime,
        end_at: datetime.datetime,
        force_redownload=False,
    ) -> pd.Series:
        """Get daily lending interest rates for a given asset from Binance, resampled to the given time bucket.

        :param asset_symbol:

            List of all valid asset symbols as at 2023-11-06 18:00 UTC:

            asset_names = [
                "BTC", "ETH", "XRP", "BNB", "TRX", "USDT", "LINK", "EOS", "ADA", "ONT",
                "USDC", "ETC", "LTC", "XLM", "XMR", "NEO", "ATOM", "DASH", "ZEC", "MATIC",
                "BUSD", "BAT", "IOST", "VET", "QTUM", "IOTA", "XTZ", "BCH", "RVN", "ZIL",
                "ONE", "ANKR", "CELR", "TFUEL", "IOTX", "HBAR", "FTM", "SXP", "BNT", "DOT",
                "REN", "ALGO", "ZRX", "THETA", "COMP", "KNC", "OMG", "KAVA", "BAND", "DOGE",
                "RLC", "WAVES", "MKR", "SNX", "YFI", "CRV", "SUSHI", "UNI", "MANA", "STORJ",
                "UMA", "JST", "AVAX", "NEAR", "FIL", "TRB", "RSR", "TOMO", "OCEAN", "AAVE",
                "SAND", "CHZ", "ARPA", "COTI", "FET", "TROY", "CHR", "ORN", "NMR", "GRT",
                "STPT", "LRC", "KSM", "ROSE", "REEF", "STMX", "ALPHA", "STX", "ENJ", "RUNE",
                "SKL", "INJ", "OXT", "CTSI", "OGN", "EGLD", "1INCH", "DODO", "LIT", "NKN",
                "MDT", "CKB", "CAKE", "SOL", "XEM", "LINA", "GLM", "XVS", "MDX", "SUPER",
                "GTC", "PUNDIX", "AUDIO", "BOND", "SLP", "TRU", "POND", "ERN", "ATA", "NULS",
                "DENT", "TVK", "DF", "FLOW", "AR", "DYDX", "MASK", "UNFI", "AXS", "LUNA",
                "SHIB", "ENS", "BAKE", "ALICE", "TLM", "ICP", "C98", "GALA", "ONG", "HIVE",
                "DAR", "IDEX", "ANT", "CLV", "WAXP", "BNX", "KLAY", "MINA", "XEC", "RNDR",
                "JASMY", "QUICK", "LPT", "AGLD", "BICO", "CTXC", "DUSK", "HOT", "SFP", "YGG",
                "FLUX", "ICX", "CELO", "BETA", "BLZ", "MTL", "PEOPLE", "QNT", "PYR", "KEY",
                "PAXG", "FRONT", "TWT", "RAD", "QI", "GMT", "APE", "BSW", "KDA", "MBL", "ASTR",
                "API3", "CTK", "WOO", "GAL", "OP", "REI", "LEVER", "LDO", "FIDA", "FLM", "BURGER",
                "AUCTION", "IMX", "SPELL", "STG", "BEL", "WING", "AVA", "LOKA", "LUNC", "PHB",
                "LOOM", "AMB", "SANTOS", "VIB", "EPX", "HARD", "USTC", "DEGO", "HIGH", "GMX",
                "LAZIO", "PORTO", "ACH", "STRAX", "KP3R", "REQ", "POLYX", "APT", "PHA", "OSMO",
                "GLMR", "MAGIC", "HOOK", "AGIX", "HFT", "CFX", "ZEN", "SSV", "LQTY", "ALCX",
                "FXS", "PERP", "TUSD", "USDP", "GNS", "JOE", "RIF", "SYN", "ID", "ARB", "OAX",
                "RDNT", "EDU", "SUI", "FLOKI", "PEPE", "COMBO", "MAV", "XVG", "PENDLE", "ARKM",
                "WLD", "T", "FDUSD", "RPL", "SEI", "CYBER", "VTHO", "WBETH", "NTRN", "HIFI",
                "CVX", "ARK", "ARDR", "ACA", "VIDT", "GHST", "GAS", "OOKI", "TIA", "POWR",
                "AERGO", "SNT", "STEEM", "MEME", "PLA", "MULTI", "UFT", "ILV"
            ]

            To see current list of all valid asset symbols, submit API request https://api1.binance.com/sapi/v1/margin/allAssets with your Binance API key.

        :param start_date:
            Start date for the data. Note this value cannot be eariler than datetime.datetime(2019,4,1) due to Binance data limitations

        """
        if not force_redownload:
            try:
                return self.get_data_parquet(
                    asset_symbol, time_bucket, start_at, end_at, is_lending=True
                )
            except:
                pass

        series = self._fetch_lending_rates(asset_symbol, start_at, end_at, time_bucket)

        path = self.get_parquet_path(
            asset_symbol, time_bucket, start_at, end_at, is_lending=True
        )
        series.to_frame(name="lending_rates").to_parquet(path)

        return series

    def _fetch_lending_rates(
        self,
        asset_symbol: str,
        start_at: datetime.datetime,
        end_at: datetime.datetime,
        time_bucket: TimeBucket,
    ) -> pd.Series:
        assert type(asset_symbol) == str, "asset_symbol must be a string"
        assert (
            type(start_at) == datetime.datetime
        ), "start_date must be a datetime.datetime object"
        assert (
            type(end_at) == datetime.datetime
        ), "end_date must be a datetime.datetime object"
        assert (
            type(time_bucket) == TimeBucket
        ), "time_delta must be a pandas Timedelta object"
        # assert start_date >= datetime.datetime(2019,4,1), "start_date cannot be earlier than 2019-04-01 due to Binance data limitations"

        monthly_timestamps = generate_monthly_timestamps(start_at, end_at)
        response_data = []

        # API calls to get the data
        for i in range(len(monthly_timestamps) - 1):
            start_timestamp = monthly_timestamps[i] * 1000
            end_timestamp = monthly_timestamps[i + 1] * 1000
            url = f"https://www.binance.com/bapi/margin/v1/public/margin/vip/spec/history-interest-rate?asset={asset_symbol}&vipLevel=0&size=90&startTime={start_timestamp}&endTime={end_timestamp}"
            response = requests.get(url)
            if response.status_code == 200:
                json_data = response.json()
                data = json_data["data"]
                if len(data) > 0:
                    response_data.extend(data)
            else:
                raise ValueError(
                    f"No data found for {asset_symbol} between {start_at} and {end_at}. Check your symbol matches with valid symbols in method description. \nResponse: {response.status_code} {response.text}"
                )

        dates = []
        interest_rates = []
        for data in response_data:
            dates.append(pd.to_datetime(data["timestamp"], unit="ms"))
            interest_rates.append(float(data["dailyInterestRate"]))

        unsampled_rates = pd.Series(data=interest_rates, index=dates).sort_index()

        # doesn't always raise error
        if unsampled_rates.empty:
            raise ValueError(
                f"No data found for {asset_symbol} between {start_at} and {end_at}. Check your symbol matches with valid symbols in method description. \nResponse: {response.status_code} {response.text}"
            )

        resampled_rates = resample_series(
            unsampled_rates, time_bucket.to_pandas_timedelta(), forward_fill=True
        )

        return resampled_rates

    def get_data_parquet(
        self,
        symbol: str,
        time_bucket: TimeBucket,
        start_at: datetime.datetime,
        end_at: datetime.datetime,
        is_lending: bool = False,
    ) -> pd.DataFrame:
        """Get parquet file for the candlestick data.

        :param symbol: Trading pair symbol E.g. ETHUSDC
        :param time_bucket: TimeBucket instance
        :param start_at: Start date of the data
        :param end_at: End date of the data
        :return: Path to the parquet file
        """
        path = self.get_parquet_path(symbol, time_bucket, start_at, end_at, is_lending)
        try:
            return pd.read_parquet(path)
        except Exception as e:
            raise e

    def get_parquet_path(
        self,
        symbol: str,
        time_bucket: TimeBucket,
        start_at: datetime.datetime,
        end_at: datetime.datetime,
        is_lending: bool = False,
    ) -> Path:
        """Get parquet path for the candlestick data.

        :param symbol: Trading pair symbol E.g. ETHUSDC
        :param time_bucket: TimeBucket instance
        :param start_at: Start date of the data
        :param end_at: End date of the data
        :return: Path to the parquet file
        """
        if is_lending:
            file_str = "lending"
        else:
            file_str = "candles"

        file = Path(
            file_str + f"-{symbol}-{time_bucket.value}-{start_at}-{end_at}.parquet"
        )
        return self.cache_directory.joinpath(file)

    def purge_cached_file(
        self,
        *,
        symbol: str = None,
        time_bucket: TimeBucket = None,
        start_at: datetime.datetime = None,
        end_at: datetime.datetime = None,
        path: Path = None,
    ) -> None:
        """Purge specific cached candle data file.

        :param symbol: Trading pair symbol E.g. ETHUSDC
        :param time_bucket: TimeBucket instance
        :param start_at: Start date of the data
        :param end_at: End date of the data
        :param path: Path to the parquet file. If not specified, it will be generated from the other parameters.
        """
        if not path:
            path = self.get_parquet_path(symbol, time_bucket, start_at, end_at)
        if path.exists():
            path.unlink()
        else:
            logger.warn(f"File {path} does not exist.")

    def purge_all_cached_data(self) -> None:
        """Purge all cached candle data. This delete all contents of a cache directory, but not the directory itself. I.e. the cache directory will be left empty

        :param path: Path to the parquet file
        """
        for item in self.cache_directory.iterdir():
            if item.is_dir():
                # Recursively delete directories
                shutil.rmtree(item)
            else:
                # Delete files
                item.unlink()


def convert_binance_lending_rates_to_supply(
    interestRates: pd.Series, multiplier: float = 0.95
) -> pd.Series:
    """Convert Binance lending rates to supply rates.

    Right now, this rate is somewhat arbitrary. It is 95% of the lending rate by default.

    :param interestRates: Series of lending interest rates
    :return: Series of supply rates
    """

    assert 0 < multiplier < 1, "Multiplier must be between 0 and 1"

    assert isinstance(
        interestRates, pd.Series
    ), f"Expected pandas Series, got {interestRates.__class__}: {interestRates}"
    assert isinstance(
        interestRates.index, pd.DatetimeIndex
    ), f"Expected DateTimeIndex, got {interestRates.index.__class__}: {interestRates.index}"
    return interestRates * multiplier


def clean_time_series_data(df: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
    """Unused for now since data from Binance data occasionally has gaps. Not a huge deal.

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
        raise ValueError(
            f"Dataframe contains uneven intervals at indices {uneven_indices}"
        )

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
