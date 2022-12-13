import pandas as pd

from eth_defi.price_oracle.oracle import TrustedStablecoinOracle

from tradingstrategy.direct_feed.candle_feed import CandleFeed
from tradingstrategy.direct_feed.reorg_mon import SyntheticReorganisationMonitor
from tradingstrategy.direct_feed.synthetic_feed import SyntheticFeed
from tradingstrategy.direct_feed.timeframe import Timeframe



def test_candle_feed_initial_load():
    """Load the first batch of trades from the chain and make candles."""

    mock_chain = SyntheticReorganisationMonitor(block_duration_seconds=12)
    mock_chain.produce_blocks(100)
    timeframe = Timeframe("1min")

    feed = SyntheticFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
        timeframe=timeframe,
        min_amount=-50,
        max_amount=50,
    )

    candle_feed =  CandleFeed(
        ["ETH-USD"],
        timeframe=timeframe,
    )

    delta = feed.backfill_buffer(100, None)
    candle_feed.apply_delta(delta)

    candles = candle_feed.get_candles_by_pair("ETH-USD")
    assert len(candles) == 21
    record = candles.iloc[0]

    assert candles.index[0] == pd.Timestamp('1970-01-01 00:00:00')
    assert candles.index[-1] == pd.Timestamp('1970-01-01 00:20:00')

    # high             193.347242914728639107124763540923595428466796875
    # low              183.275727112817804709266056306660175323486328125
    # close             191.49711213319125135967624373733997344970703125
    # exchange_rate                                                  1.0
    # start_block                                                      1
    # end_block                                                        4
    # volume                               606.3863506339667104327872948
    # avg_trade                                                25.266098
    # buys                                                            18
    # sells                                                            6
    # Name: 1970-01-01 01:00:00, dtype: object

    assert record["high"] > 0
    assert record["low"] > 0
    assert record["close"] > 0
    assert record["exchange_rate"] == 1
    assert record["volume"] > 0
    assert record["avg_trade"] < 50
    assert record["buys"] > 0
    assert record["sells"] > 0
    assert record["start_block"] == 1
    assert record["end_block"] == 4
    assert candles.index[0] == pd.Timestamp("1970-1-1")


def test_candle_feed_increment():
    """Load the first batch of trades from the chain and make candles."""

    mock_chain = SyntheticReorganisationMonitor(block_duration_seconds=12)
    mock_chain.produce_blocks(100)
    timeframe = Timeframe("1min")

    feed = SyntheticFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
        timeframe=timeframe,
        min_amount=-50,
        max_amount=50,
    )

    candle_feed =  CandleFeed(
        ["ETH-USD"],
        timeframe=timeframe,
    )

    delta = feed.backfill_buffer(100, None)
    candle_feed.apply_delta(delta)

    mock_chain.produce_blocks(1)
    delta = feed.perform_duty_cycle()
    candle_feed.apply_delta(delta)

    candles = candle_feed.get_candles_by_pair("ETH-USD")
    assert candles.index[0] == pd.Timestamp('1970-01-01 00:00:00')
    assert candles.index[-1] == pd.Timestamp('1970-01-01 00:20:00')
