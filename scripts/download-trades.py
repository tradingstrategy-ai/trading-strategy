"""An example script to download all Uniswap trades and preprocess data.

- Downlaod the all trades Parquet file (~100 GB)

- Split it to `trades-{chain}.parquet` files
  that are manageable sized files

- Sort data for it to be in the monotonic block number > tx hash > log index 
  order

You will need more than 200 GB free disk space to run this script/
"""

import os
import shutil
import logging

DISK_SPACE_THRESHOLD = 200 * 10**9

DOWNLOAD_PATH = os.environ.get("DOWNLOAD_PATH") or os.getcwd()

print(f"Download and working directory is {DOWNLOAD_PATH}")

total, used, free = shutil.disk_usage(DOWNLOAD_PATH)

assert free > DISK_SPACE_THRESHOLD, f"Not enough free disk space, we have {DISK_SPACE_THRESHOLD,}"


