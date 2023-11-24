"""An example script to download Binance daily data for given set of pairs.

- Download spot market OHLCV data from Binance public API endpoint

- Create a combined Parquet file for all pairs.
"""
import datetime
import os

import pandas as pd
from tqdm.auto import tqdm

from tradingstrategy.binance_data import BinanceDownloader
from tradingstrategy.timebucket import TimeBucket

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
pair_hash = hash(pairs)
fpath = f"/tmp/binance-candles-{time_bucket.value}-{pair_hash}.parquet"

downloader = BinanceDownloader()

parts = []
total_size = 0

with tqdm(total=len(pairs)) as progress_bar:
    for symbol in pairs:

        end = datetime.datetime.utcnow() - datetime.timedelta(hours=24)

        # Fetch data for this pair, or use the cached file if already downloaded earlier
        df = downloader.fetch_candlestick_data(symbol, time_bucket, start, end)

        # Label the dataset with the ticker we downloaded,
        # so we can group pairs in the flattened file
        df["pair_id"] = symbol

        # Count the cached file size
        path = downloader.get_parquet_path(symbol, time_bucket, start, end)
        total_size += os.path.getsize(path)

        progress_bar.set_postfix({"pair": symbol, "total_size (MBytes)": total_size / (1024**2)})
        progress_bar.update()

flattened_df = pd.concat(parts)
flattened_df = flattened_df.reset_index().set_index("timestamp")  # Get rid of grouping
flattened_df.to_parquet(fpath)
print(f"Wrote {fpath} {os.path.getsize(fpath):,} bytes")



