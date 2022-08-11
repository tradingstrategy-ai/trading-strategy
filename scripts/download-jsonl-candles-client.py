"""A sample script to download JSONL candle data.

Use Trading Strategy client and display a progress bar.
"""

import os
import logging
import requests

from tradingstrategy.client import Client
from tradingstrategy.timebucket import TimeBucket

logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])
logging.getLogger("matplotlib").disabled = True

api_url = "https://tradingstrategy.ai/api"

bnb_busd_params = {
    "chain_slug": "binance",
    "exchange_slug": "pancakeswap-v2",
    "pair_slug": "bnb-busd",
}

resp = requests.get(f"{api_url}/pair-details", bnb_busd_params)
assert resp.status_code == 200, f"Got {resp.text}"

bnb_busd = resp.json()

print("Pair #1", bnb_busd["summary"]["pair_name"], bnb_busd["summary"]["pair_id"])

eth_usdc_params = {
    "chain_slug": "ethereum",
    "exchange_slug": "uniswap-v2",
    "pair_slug": "eth-usdc",
}

resp = requests.get(f"{api_url}/pair-details", eth_usdc_params)
assert resp.status_code == 200, f"Got {resp.text}"

eth_usdc = resp.json()

print("Pair #2", eth_usdc["summary"]["pair_name"], eth_usdc["summary"]["pair_id"])

pair_ids = {eth_usdc["summary"]["pair_id"], bnb_busd["summary"]["pair_id"]}

client = Client.create_live_client(os.environ["TRADING_STRATEGY_API_KEY"])

print("Fetching candle data")
candles = client.fetch_candles_by_pair_ids(
    pair_ids,
    TimeBucket.m5,
    progress_bar_description="Loading test candle data",
)

assert len(candles) > 0