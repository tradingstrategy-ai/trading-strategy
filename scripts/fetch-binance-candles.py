"""An example script to download Binance daily data for given set of pairs.

- Download spot market OHLCV data from Binance public API endpoint

- Create a combined Parquet file for all pairs.
"""
import datetime
import os

import pandas as pd
from IPython.core.display_functions import display
from tqdm.auto import tqdm

from tradingstrategy.binance_data import BinanceDownloader
from tradingstrategy.timebucket import TimeBucket

<<<<<<< Updated upstream
# Choose 10 well-known pairs
pairs = {
    "ETHUSDT",
    "BTCUSDT",
    "LINKUSDT",
    "MATICUSDT",
    "AAVEUSDT",
    "COMPUSDT",
    "MKRUSDT",
    "BNBUSDT",
    "AVAXUSDT",
    "CAKEUSDT",
    "SNXUSDT",
    "CRVUSDT",
}

time_bucket = TimeBucket.d1
pair_hash = hex(hash("".join(sorted(list(pairs)))))  # If we change any of pairs the filename must change
fpath = f"/tmp/binance-candles-{time_bucket.value}-{pair_hash}.parquet"

=======
time_bucket = TimeBucket.h1
fpath = f"/tmp/binance-candles-{time_bucket.value}.parquet"
>>>>>>> Stashed changes
downloader = BinanceDownloader()

parts = []
total_size = 0

with tqdm(total=len(pairs)) as progress_bar:
    for symbol in pairs:

        start = downloader.fetch_approx_asset_trading_start_date(symbol)
        end = datetime.datetime.utcnow() - datetime.timedelta(hours=24)

        # Fetch data for this pair, or use the cached file if already downloaded earlier
        df = downloader.fetch_candlestick_data(symbol, time_bucket, start, end)

        # Label the dataset with the ticker we downloaded,
        # so we can group pairs in the flattened file
        df["pair_id"] = symbol
        df["timestamp"] = df.index.copy()  # We need to preserve this in the flattening later

        # Count the cached file size
        path = downloader.get_parquet_path(symbol, time_bucket, start, end)
        total_size += os.path.getsize(path)

        parts.append(df)

        progress_bar.set_postfix({"pair": symbol, "total_size (MBytes)": total_size / (1024**2)})
        progress_bar.update()

flattened_df = pd.concat(parts)
flattened_df = flattened_df.set_index("timestamp")  # Get rid of grouping
flattened_df = flattened_df.sort_values(by=["pair_id", "timestamp"])  # Arrange data for maximum columnar compression
flattened_df.to_parquet(fpath)

first_candle_at = flattened_df.index.min()
last_candle_at = flattened_df.index.max()

# Display sample data, the first entry for each trading pair
sample_data_df = flattened_df.drop_duplicates("pair_id")
display(sample_data_df)

print(f"Total {len(flattened_df):,} candles, between {first_candle_at} - {last_candle_at}, wrote {fpath} {os.path.getsize(fpath):,} bytes")



