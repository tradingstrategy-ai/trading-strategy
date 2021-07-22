"""Single pair backtesting with Fastquant"""

import pandas as pd

from fastquant import backtest

from capitalgram.chain import ChainId
from capitalgram.client import Capitalgram
from capitalgram.candle import CandleBucket
from capitalgram.pair import PairUniverse, get_one_pair_from_pandas_universe

capitalgram = Capitalgram.create_jupyter_client()

# Exchange map data is so small it does not need any decompression
exchange_universe = capitalgram.fetch_exchange_universe()

# Get Sushiswap out from the exchange pile
sushi_swap = exchange_universe.get_by_name_and_chain(ChainId.ethereum, "sushiswap")

# Decompress the pair dataset to and extract SUSHI-USDC id
all_pairs = capitalgram.fetch_pair_universe().to_pandas()
sushi_usdc = get_one_pair_from_pandas_universe(all_pairs, sushi_swap.exchange_id, "SUSHI", "USDC")

assert sushi_usdc is not None, "No SUSHI-USDC pair found in the data"

print(f"Running backtest for {sushi_usdc.base_token_symbol} - {sushi_usdc.quote_token_symbol}, pair id {sushi_usdc.pair_id}")

# Get 24h candles and cull down leaving only SUSHI-USDC
candles_24h = capitalgram.fetch_all_candles(CandleBucket.h24).to_pandas()
sushi_candles = candles_24h.loc[candles_24h["pair_id"] == sushi_usdc.pair_id ]

# Prepare volume and timeseries index for backtest()
sushi_candles["volume"] = sushi_candles["buy_volume"] + sushi_candles["sell_volume"]
sushi_candles = sushi_candles.set_index(pd.DatetimeIndex(sushi_candles["timestamp"]))

# Test a SUSHI-USDC strategy
# Simple Moving Average Crossover (15 day MA vs 40 day MA)
print(f"Total {len(sushi_candles)} candles, running the backtest")
backtest('smac', sushi_candles, fast_period=15, slow_period=40)

