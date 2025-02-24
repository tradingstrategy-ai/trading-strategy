"""Load TVL data as Parquet."""
import datetime
from pathlib import Path

import pytest
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.transport.cache import APIError


def test_load_tvl_parquet(
    persistent_test_client: Client,
    default_pair_universe,
):
    """Load CLMM data for two pairs on Uniswap v3."""

    client = persistent_test_client

    # Clean cache before first download attempt
    for p in Path(client.transport.cache_path).glob("min-tvl-*"):
        p.unlink()

    exchange_universe = client.fetch_exchange_universe()
    base_uni_v2 = exchange_universe.get_by_chain_and_slug(ChainId.base, "uniswap-v2")
    base_uni_v3 = exchange_universe.get_by_chain_and_slug(ChainId.base, "uniswap-v3")

    start = datetime.datetime(2025, 1, 1)
    end = datetime.datetime(2025, 2, 1)

    # Disable retries, as we want to fail on the first request
    retry_policy = Retry(
        total=0,
    )
    adapter = HTTPAdapter(max_retries=retry_policy)
    client.transport.requests.mount('https://', adapter)

    df = client.fetch_tvl(
        mode="min_tvl",
        bucket=TimeBucket.d1,
        start_time=start,
        end_time=end,
        exchange_ids={base_uni_v2.exchange_id, base_uni_v3.exchange_id},
        min_tvl=5_000_000,
    )
    assert df.attrs["cached"] is False, f"Cached at {df.attrs['path']}"
    assert len(df) == 562

    assert "pair_id" in df.columns
    assert "bucket" in df.columns
    assert "open" in df.columns
    assert "high" in df.columns
    assert "low" in df.columns
    assert "close" in df.columns

    # Do second round, the file should be now in a cache
    df = client.fetch_tvl(
        mode="min_tvl",
        bucket=TimeBucket.d1,
        start_time=start,
        end_time=end,
        exchange_ids={base_uni_v2.exchange_id, base_uni_v3.exchange_id},
        min_tvl=5_000_000,
    )
    assert df.attrs["cached"] is True
    assert df.attrs["filesize"] > 0
    assert df.attrs["path"] is not None

