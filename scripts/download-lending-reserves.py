"""Check lending reserve data for Aave.

- Manually call the endpoint to fetch the reserves JSON blob
"""
import os
import logging
import sys

import requests
import tabulate

from tradingstrategy.client import Client
from tradingstrategy.timebucket import TimeBucket

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

api_key = os.environ["TRADING_STRATEGY_API_KEY"]
assert api_key.startswith("secret-token:"), f"API key must start with secret-token: - we got: {api_key[0:8]}..."

api_url = "https://tradingstrategy.ai/api"

headers = {
    "Authorization": api_key,
}

resp = requests.get(
    f"{api_url}/lending-reserve-universe",
    headers=headers,
)
assert resp.status_code == 200, f"Got {resp.text}"

reserve = resp.json()["reserves"]
data = []
for reserve_id, reserve in reserve.items():
    data.append(
        {
            "reserve_id": reserve_id,
           "reserve_slug": reserve["reserve_slug"],
            "protocol_slug": reserve["protocol_slug"],
            "chain_slug": reserve["chain_slug"],
            "atoken_symbol": reserve["atoken_symbol"],
            "liquidation_threshold: ": reserve.get("liquidation_threshold", "-"),
        }
    )

print(tabulate.tabulate(data, headers="keys", tablefmt="fancy_grid"))
