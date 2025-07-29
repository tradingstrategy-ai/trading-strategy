"""Check lending reserve data for Aave.

- Manually call the endpoint to fetch the reserves JSON blob
"""
import os
import logging
import sys

import requests
import tabulate

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
# {'reserve_id': 51, 'reserve_slug': '1inch', 'protocol_slug': 'aave_v3', 'protocol_name': 'Aave V3', 'chain_id': 1, 'chain_slug': 'ethereum', 'chain_name': 'Ethereum', 'asset_id': 25385, 'asset_name': '1INCH Token', 'asset_symbol': '1INCH', 'asset_address': '0x111111111117dc0aa78b770fa6a738034120c302', 'asset_decimals': 18, 'atoken_id': 2698418, 'atoken_symbol': 'aEth1INCH', 'atoken_address': '0x71aef7b30728b9bb371578f36c5a1f1502a5723e', 'atoken_decimals': 18, 'stable_debt_token_id': 2698421, 'stable_debt_token_address': '0x4b62bfaff61ab3985798e5202d2d167f567d0bcd', 'variable_debt_token_id': 2698420, 'variable_debt_token_symbol': 'variableDebtEth1INCH', 'variable_debt_token_address': '0xa38fca8c6bf9bda52e76eb78f08caa3be7c5a970', 'variable_debt_token_decimals': 18, 'interest_rate_strategy_address': '0xf6733b9842883bfe0e0a940ea2f572676af31bde', 'additional_details': {'supply_apr_latest': None, 'stable_borrow_apr_latest': None, 'variable_borrow_apr_latest': None, 'aggregated_reserve_data': None, 'base_currency_info': None, 'block_number': None, 'ltv': 0.57, 'liquidation_threshold': 0.67}}
for reserve_id, reserve in reserve.items():
    data.append(
        {
            "reserve_id": reserve_id,
           "reserve_slug": reserve["reserve_slug"],
            "protocol_slug": reserve["protocol_slug"],
            "chain_slug": reserve["chain_slug"],
            "atoken_symbol": reserve["atoken_symbol"],
            "liquidation_threshold: ": reserve["additional_details"].get("liquidation_threshold", "-"),
        }
    )

print(tabulate.tabulate(data, headers="keys", tablefmt="fancy_grid"))
