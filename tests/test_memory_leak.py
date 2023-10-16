"""Manual memory leak test."""

import sys
import gc
import time
from pathlib import PosixPath

import pytest
from tradingstrategy.client import Client

from pyarrow import parquet as pq


TEST_PARQUET_FILE = PosixPath('/tmp/trading-strategy-tests/memory-leak.parquet')


@pytest.mark.skipif(
    TEST_PARQUET_FILE.exists() == False,
    reason="Manual PyArrow memory leak test, see the file for run instructions"
)
def test_memory_leak(persistent_test_client: Client):
    """Load pair parquet file repeatly and see how it affects RSS.

    To test:

        cp /tmp/trading-strategy-tests/pair-universe.parquet /tmp/trading-strategy-tests/memory-leak.parquet
        pytest -k test_memory_leak

    """
    import psutil
    client = persistent_test_client

    p = psutil.Process()

    for i in range(0, 180):

        file_path = TEST_PARQUET_FILE
        data = pq.read_table(file_path, memory_map=True, use_threads=False)

        rss = p.memory_info().rss
        print(f"RSS is {rss:,}")
        gc.collect()
        import pyarrow
        pool = pyarrow.default_memory_pool()
        pool.release_unused()
        time.sleep(0.1)



