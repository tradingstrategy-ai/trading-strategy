"""CLMM data tests."""

import datetime
from pathlib import Path

import pytest
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.transport.cache import APIError


def test_load_clmm_two_pairs_mixed_exchange(
    persistent_test_client: Client,
    default_pair_universe,
):
    """Load CLMM data for two pairs on Uniswap v3."""

    client = persistent_test_client

    # Clean cache before first download attempt
    for p in Path(client.transport.cache_path).glob("clmm-*"):
        p.unlink()

    pair_universe = default_pair_universe

    pair = pair_universe.get_pair_by_human_description(
        (ChainId.ethereum, "uniswap-v3", "WETH", "USDC", 0.0005)
    )

    pair_2 = pair_universe.get_pair_by_human_description(
        (ChainId.ethereum, "uniswap-v3", "DAI", "USDC", 0.0001)
    )

    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 2, 1)

    # Disable retries, as we want to fail on the first request
    retry_policy = Retry(
        total=0,
    )
    adapter = HTTPAdapter(max_retries=retry_policy)
    client.transport.requests.mount('https://', adapter)

    clmm_df = client.fetch_clmm_liquidity_provision_candles_by_pair_ids(
        [pair.pair_id, pair_2.pair_id],
        TimeBucket.d1,
        start_time=start,
        end_time=end,
    )
    assert clmm_df.attrs["cached"] is False, f"Cached at {clmm_df.attrs['path']}"
    assert len(clmm_df) == 64

    # Do second round, the file should be now in a cache

    clmm_df = client.fetch_clmm_liquidity_provision_candles_by_pair_ids(
        [pair.pair_id, pair_2.pair_id],
        TimeBucket.d1,
        start_time=start,
        end_time=end,
    )
    assert clmm_df.attrs["cached"] is True
    assert clmm_df.attrs["filesize"] > 0
    assert clmm_df.attrs["path"] is not None


def test_load_clmm_bad_pair(
    persistent_test_client: Client,
    default_pair_universe,
):
    """Attempt load CLMM data for Uniswap v2 pair."""

    client = persistent_test_client

    pair_universe = default_pair_universe

    pair = pair_universe.get_pair_by_human_description(
        (ChainId.ethereum, "uniswap-v2", "WETH", "USDC")
    )

    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 2, 1)

    # Disable retries, as we want to fail on the first request
    retry_policy = Retry(
        total=0,
    )
    adapter = HTTPAdapter(max_retries=retry_policy)
    client.transport.requests.mount('https://', adapter)

    with pytest.raises(APIError) as exc_info:
        client.fetch_clmm_liquidity_provision_candles_by_pair_ids(
            [pair.pair_id],
            TimeBucket.d1,
            start_time=start,
            end_time=end,
        )

    e = exc_info.value
    assert "CandleLookupError" in str(e)

