"""Test creating aggregates of volume data across multiple pairs."""
import datetime

import pandas as pd
import pytest

from tradingstrategy.chain import ChainId
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.utils.aggregate_ohlcv import calculate_volume_weighted_ohlcv, aggregate_ohlcv_across_pairs
from tradingstrategy.utils.forward_fill import forward_fill, xxx_forward_fill

# pair_id, timestamp, open, high, low, close, liquidity
example_data = [
    (1, pd.Timestamp("2020-01-01"), 100, 100, 100, 100, 500, 10),
    (1, pd.Timestamp("2020-02-02"), 100, 100, 100, 100, 500, 10),

    (2, pd.Timestamp("2020-01-01"), 110, 110, 110, 110, 250, 20),
    (2, pd.Timestamp("2020-02-02"), 110, 110, 110, 110, 250, 20),

    (3, pd.Timestamp("2020-02-02"), 200, 200, 200, 200, 1000, 30),
]


@pytest.fixture(scope="module")
def pair_timestamp_df() -> pd.DataFrame:
    df = pd.DataFrame(example_data, columns=["pair_id", "timestamp", "open", "high", "low", "close", "volume", "liquidity"])
    df = df.set_index("timestamp")
    return df


def test_calculate_volume_weighted_ohlc(pair_timestamp_df: pd.DataFrame):
    aggregate_ohlcvl = calculate_volume_weighted_ohlcv(pair_timestamp_df)

    #                   open        high         low       close  volume  liquidity
    # timestamp
    # 2020-01-01  103.333333  103.333333  103.333333  103.333333     750         30
    # 2020-02-02  158.571429  158.571429  158.571429  158.571429    1750         60

    assert aggregate_ohlcvl["open"][pd.Timestamp("2020-01-01")] == pytest.approx(103.333333)
    assert aggregate_ohlcvl["volume"][pd.Timestamp("2020-01-01")] == pytest.approx(750)
    assert aggregate_ohlcvl["liquidity"][pd.Timestamp("2020-01-01")] == pytest.approx(30)
    assert aggregate_ohlcvl["liquidity"][pd.Timestamp("2020-02-02")] == pytest.approx(60)



def test_aggregate_ohlcv_across_pairs(persistent_test_client):
    """Create aggregated price/volume feed.

    - Aggregate all WETH/USDC pools on Uniswap v3 on Ethereum
    """

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.ethereum, "uniswap-v3")

    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [
                (exchange.chain_id, exchange.exchange_slug, "WETH", "USDC", 0.0005),
                (exchange.chain_id, exchange.exchange_slug, "WETH", "USDC", 0.0030),
                (exchange.chain_id, exchange.exchange_slug, "WETH", "USDC", 0.0100)
            ],
        )
    pair_ids = {p.pair_id for p in pair_universe.iterate_pairs()}
    candles_df = client.fetch_candles_by_pair_ids(
        pair_ids,
        TimeBucket.d7,
        start_time=datetime.datetime(2024, 1, 1),
        end_time=datetime.datetime(2024, 3, 1)
    )
    candles_df = candles_df.groupby("pair_id")
    candles_df = forward_fill(candles_df, "W")

    # fetch_all_liquidity_samples() unnecessary heavy here
    # TODO: Change to dynamic fetch method in the future
    liquidity_df = client.fetch_all_liquidity_samples(TimeBucket.d7).to_pandas()
    liquidity_df = liquidity_df.loc[liquidity_df["pair_id"].isin(pair_ids)]
    liquidity_df = liquidity_df.set_index("timestamp").groupby("pair_id")
    liquidity_df_ff = forward_fill(liquidity_df, "W", columns=("close",))  # Only close liquidity column needd


    aggregated_df = aggregate_ohlcv_across_pairs(
        pair_universe,
        candles_df,
        liquidity_df_ff["close"],
    )

    #                    open         high          low        close        volume     liquidity                                       aggregate_id  base quote                     pair_ids
    # timestamp
    # 2024-01-07  2280.677077  2440.676248  2161.803878  2221.170697  1.605549e+09  2.041719e+08  1-WETH-USDC-0xc02aaa39b223fe8d0a0e5c4f27ead908...  WETH  USDC  [2697600, 2697585, 2697765]
    # 2024-01-14  2221.175063  2728.023200  2171.172254  2471.981456  2.505056e+09  2.162977e+08  1-WETH-USDC-0xc02aaa39b223fe8d0a0e5c4f27ead908...  WETH  USDC  [2697600, 2697585, 2697765]
    # 2024-01-21  2472.138005  2612.277012  2400.088044  2454.585816  9.976005e+08  2.243083e+08  1-WETH-USDC-0xc02aaa39b223fe8d0a0e5c4f27ead908...  WETH  USDC  [2697600, 2697585, 2697765]
    # 2024-01-28  2454.663550  2462.933336  2155.359461  2256.987915  1.461781e+09  2.081431e+08  1-WETH-USDC-0xc02aaa39b223fe8d0a0e5c4f27ead908...  WETH  USDC  [2697600, 2697585, 2697765]
    # 2024-02-04  2256.392322  2388.478129  2230.724865  2288.710122  1.049938e+09  2.115997e+08  1-WETH-USDC-0xc02aaa39b223fe8d0a0e5c4f27ead908...  WETH  USDC  [2697600, 2697585, 2697765]
    # 2024-02-11  2288.772568  2549.559342  2269.939686  2506.784720  1.125211e+09  2.101656e+08  1-WETH-USDC-0xc02aaa39b223fe8d0a0e5c4f27ead908...  WETH  USDC  [2697600, 2697585, 2697765]
    # 2024-02-18  2507.241322  2899.496867  2439.057433  2879.361399  1.391992e+09  1.891732e+08  1-WETH-USDC-0xc02aaa39b223fe8d0a0e5c4f27ead908...  WETH  USDC  [2697600, 2697585, 2697765]
    # 2024-02-25  2880.880840  3115.638917  2850.855441  3109.890699  1.689866e+09  2.595613e+08  1-WETH-USDC-0xc02aaa39b223fe8d0a0e5c4f27ead908...  WETH  USDC  [2697600, 2697585, 2697765]
    # 2024-03-03  3111.652078  3611.985609  3040.088687  3490.146173  2.901353e+09  2.764194e+08  1-WETH-USDC-0xc02aaa39b223fe8d0a0e5c4f27ead908...  WETH  USDC  [2697600, 2697585, 2697765]

    assert len(aggregated_df) == 9
