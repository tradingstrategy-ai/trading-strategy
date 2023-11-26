"""A sample script to download JSONL candle data.

Request all historical data for

- 15m candles

- ETH/USDC

- BNB/BUSD

The download JSONL binary size is 28 Mbytes
"""

import datetime
from collections import defaultdict

import requests
import jsonlines

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

id_list = (eth_usdc["summary"]["pair_id"], bnb_busd["summary"]["pair_id"])

params = {
    "pair_ids": ",".join(str(i) for i in id_list),
    "time_bucket": "15m",
}

# Peak the URL, so the code example
# is easier to understand
print("Opening OHLCV data stream")
resp = requests.Request("GET", f"{api_url}/candles-jsonl", params=params)
prep = resp.prepare()
print("The final URL is", prep.url)

# Open the actual HTTP connection
resp = requests.get(f"{api_url}/candles-jsonl", params=params)
reader = jsonlines.Reader(resp.raw)

# Iterate the resulting response
# using jsonlines reader.
# We start to decode incoming data on the first arrived byte
# and keep decoding while streaming the response.
# https://stackoverflow.com/a/60846477/315168
print("Iterating response")
candle_data = defaultdict(list)
for item in reader:
    pair_id = item["p"]
    candle_data[pair_id].append(item)

eth_usdc_candles = candle_data[eth_usdc["summary"]["pair_id"]]
first_candle = naive_utcfromtimestamp(eth_usdc_candles[0]["ts"])
last_candle = naive_utcfromtimestamp(eth_usdc_candles[-1]["ts"])
bnb_busd_candles = candle_data[bnb_busd["summary"]["pair_id"]]

print(f"ETH-USDC has {len(eth_usdc_candles):,} candles from {first_candle} to {last_candle}")
print(f"BNB-BUSD has {len(bnb_busd_candles):,} candles")
