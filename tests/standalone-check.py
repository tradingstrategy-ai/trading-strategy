"""A script to check that the package works with normal pip install without optional dependencies.

- Will catch any import errors

See scripts/standalone-test.sh
"""

print("Starting standalone check")

import os

import pandas as pd

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket

# For use the given test API key
client = Client.create_live_client(
    settings_path=None,
    api_key=os.environ["TRADING_STRATEGY_API_KEY"],
)
# Load pairs in all exchange
exchange_universe = client.fetch_exchange_universe()
pairs_df = client.fetch_pair_universe().to_pandas()

pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

pair_ids = [
    pair_universe.get_pair_by_human_description([ChainId.ethereum, "uniswap-v3", "WETH", "USDC", 0.0005]).pair_id,
]

start = pd.Timestamp.utcnow() - pd.Timedelta("3d")
end = pd.Timestamp.utcnow()

clmm_df = client.fetch_clmm_liquidity_provision_candles_by_pair_ids(
    pair_ids,
    TimeBucket.d1,
    start_time=start,
    end_time=end,
)

assert len(clmm_df) > 0