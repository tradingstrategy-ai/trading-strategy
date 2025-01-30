"""Example of using /top endpoint to load TokenSniffer score"""
import logging
import sys

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.top import TopPairMethod

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
)

client = Client.create_live_client()
chain_id = ChainId.binance
token_address = "0xb1aed8969439efb4ac70d7397ba90b276587b27d"
token_addresses = {token_address}

top_pair_reply = client.fetch_top_pairs(
    {chain_id},
    addresses=token_addresses,
    method=TopPairMethod.by_token_addresses,
    min_volume_24h_usd=0,
    risk_score_threshold=0,
)

pair_data = top_pair_reply.find_pair_data_for_token(token_address)
print(pair_data)
import ipdb ; ipdb.set_trace()
