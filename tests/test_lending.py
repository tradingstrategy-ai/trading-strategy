
"""Client lending dataset download and integrity tests"""

import logging
from pathlib import Path

from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.client import Client
from tradingstrategy.lending import LendingCandleType

logger = logging.getLogger(__name__)


def test_client_download_lending_reserves_all_time(client: Client, cache_path: str):
    """Download all available data on lending protocol reserves."""
    df = client.fetch_lending_reserves_all_time()
    # Check we cached the file correctly
    assert Path(f"{cache_path}/lending-reserves-all.parquet").exists()
    assert len(df) > 100


def test_client_download_lending_reserve_universe(client: Client, cache_path: str):
    universe = client.fetch_lending_reserve_universe()
    # Check we cached the file correctly
    assert Path(f"{cache_path}/lending-reserve-universe.json").exists()
    assert len(universe.reserves) > 50


def test_client_fetch_lending_candles(client: Client, cache_path: str):
    df = client.fetch_lending_candles_by_reserve_id(1, TimeBucket.h1)
    assert len(df) > 50
    assert len(list(Path(cache_path).glob("lending-candles-1h-between-any-and-any-*.parquet"))) == 1

    df = client.fetch_lending_candles_by_reserve_id(3, TimeBucket.d1, candle_type=LendingCandleType.supply_apr)
    assert len(df) > 50
    assert len(list(Path(cache_path).glob("lending-candles-1d-between-any-and-any-*.parquet"))) == 1
