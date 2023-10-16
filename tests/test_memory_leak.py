"""Manual memory leak test."""

import sys
import gc
import time

from tradingstrategy.client import Client

from pyarrow import parquet as pq


def test_memory_leak(persistent_test_client: Client):
    """Load trading pair and lending data for the same backtest"""
    import psutil
    client = persistent_test_client

    p = psutil.Process()

    for i in range(0, 180):
        rss = p.memory_info().rss
        # data = client.fetch_pair_universe()
        file_path = client.transport.get_cached_item("pair-universe.parquet")
        data = pq.read_table(file_path, memory_map=False, use_threads=True)

        print("RSS is ", rss)
        gc.collect()
        import pyarrow
        pool = pyarrow.default_memory_pool()
        pool.release_unused()
        time.sleep(0.1)


