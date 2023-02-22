"""Test the speed of different methods to read liquidity data."""

import datetime
import datetime
import sys
import timeit

import pandas as pd

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.liquidity import GroupedLiquidityUniverse, LiquidityDataUnavailable, ResampledLiquidityUniverse
from tradingstrategy.pair import PandasPairUniverse, filter_for_exchanges
from tradingstrategy.timebucket import TimeBucket

client = Client.create_jupyter_client()

exchange_universe = client.fetch_exchange_universe()
sushi_swap = exchange_universe.get_by_chain_and_name(ChainId.ethereum, "sushi")

raw_pairs = client.fetch_pair_universe().to_pandas()
filtered_pairs = filter_for_exchanges(raw_pairs, {sushi_swap})

print("Total", len(filtered_pairs), "pairs")
pair_universe = PandasPairUniverse(filtered_pairs, build_index=True)

# Filter down to all liquidity data on SushiSwap
# Around ~1M samples
raw_liquidity_samples = client.fetch_all_liquidity_samples(TimeBucket.h4).to_pandas()
raw_liquidity_samples = raw_liquidity_samples.loc[raw_liquidity_samples["pair_id"].isin(filtered_pairs["pair_id"])]
liq_uni = GroupedLiquidityUniverse(raw_liquidity_samples)

resampled_liq_uni = ResampledLiquidityUniverse(raw_liquidity_samples)

grouped_by_date = raw_liquidity_samples

# The duration of the backtesting period
start_at = datetime.datetime(2022, 11, 1)
end_at = datetime.datetime(2022, 11, 10)

print("Total", len(pair_universe.pair_map.keys()), "pairs")

def method1():
    for when in pd.date_range(start_at, end_at,freq="4h"):
        hits = 0
        for pair_id in pair_universe.pair_map.keys():
            try:
                sample = liq_uni.get_liquidity_with_tolerance(pair_id, when, tolerance=pd.Timedelta("4w"))
                hits += 1
            except LiquidityDataUnavailable:
                pass
        print(when, "hits", hits)


def method2():
    for when in pd.date_range(start_at, end_at,freq="4h"):
        hits = 0
        for pair_id in pair_universe.pair_map.keys():
            try:
                sample = resampled_liq_uni.get_liquidity_fast(pair_id, when)
                hits += 1
            except LiquidityDataUnavailable:
                pass
        print(when, "hits", hits)



#import cProfile
#p = cProfile.Profile()
#p.runcall(method2)
#p.print_stats(sort="cumtime")
#sys.exit(1)

print("Method 2")
time2 = timeit.timeit(method2, number=1)

print("Method 1")
time1 = timeit.timeit(method1, number=1)

print("Time 1", time1)
print("Time 2", time2)
print("Speedup", time1 / time2)


