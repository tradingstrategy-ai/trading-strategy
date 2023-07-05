
"""Client dataset download and integrity tests"""

import os
import logging
from pathlib import Path

from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.client import Client
from tradingstrategy.chain import ChainId

logger = logging.getLogger(__name__)


def test_client_download_all_lending_reserves(client: Client, cache_path: str):
    """Download all available data on lending protocol reserves."""
    df = client.fetch_all_lending_reserves()
    # Check we cached the file correctly
    assert Path(f"{cache_path}/lending-reserves-all.parquet").exists()
    assert len(df) > 100


def test_client_download_lending_reserve_universe(client: Client, cache_path: str):
    universe = client.fetch_lending_reserve_universe()
    # Check we cached the file correctly
    assert Path(f"{cache_path}/lending-reserve-universe.json").exists()
    assert len(universe.reserves) > 50


def test_client_fetch_lending_candles(client: Client):
    df = client.fetch_lending_candles_by_reserve_id(1, TimeBucket.h1)
    print(df)

