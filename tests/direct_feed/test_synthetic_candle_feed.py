import pandas as pd

from eth_defi.price_oracle.oracle import TrustedStablecoinOracle, FixedPriceOracle

from tradingstrategy.direct_feed.candle_feed import CandleFeed
from eth_defi.event_reader.reorganisation_monitor import MockChainAndReorganisationMonitor
from tradingstrategy.direct_feed.synthetic_feed import SyntheticTradeFeed
from tradingstrategy.direct_feed.timeframe import Timeframe



def test_candle_feed_initial_load():
    """Load the first batch of trades from the chain and make candles."""

    mock_chain = MockChainAndReorganisationMonitor(block_duration_seconds=12)
    mock_chain.produce_blocks(100)
    timeframe = Timeframe("1min")

    feed = SyntheticTradeFeed(
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
    """Load trades incrementally."""

    mock_chain = MockChainAndReorganisationMonitor(block_duration_seconds=12)
    mock_chain.produce_blocks(100)
    timeframe = Timeframe("1min")

    feed = SyntheticTradeFeed(
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

    # Add 1 block
    mock_chain.produce_blocks(1)
    delta = feed.perform_duty_cycle()
    candle_feed.apply_delta(delta)

    candles = candle_feed.get_candles_by_pair("ETH-USD")
    assert candles.index[0] == pd.Timestamp('1970-01-01 00:00:00')
    assert candles.index[-1] == pd.Timestamp('1970-01-01 00:20:00')

    # Add 100 blocks
    mock_chain.produce_blocks(100)
    delta = feed.perform_duty_cycle()
    candle_feed.apply_delta(delta)

    candles = candle_feed.get_candles_by_pair("ETH-USD")
    assert candles.index[0] == pd.Timestamp('1970-01-01 00:00:00')
    assert candles.index[-1] == pd.Timestamp('1970-01-01 00:40:00')


def test_candle_feed_fork():
    """Load trades with a chain fork."""

    mock_chain = MockChainAndReorganisationMonitor(block_duration_seconds=12, check_depth=100)
    mock_chain.produce_blocks(100)
    timeframe = Timeframe("1min")

    feed = SyntheticTradeFeed(
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
    assert candle_feed.get_last_block_number() == 100

    # Fork the chain
    mock_chain.produce_fork(70, fork_marker="0x8888")
    delta = feed.perform_duty_cycle()
    candle_feed.apply_delta(delta)
    assert delta.start_block == 70
    assert delta.end_block == 100
    assert delta.reorg_detected

    candles = candle_feed.get_candles_by_pair("ETH-USD")
    assert len(candles) == 21
    assert candle_feed.get_last_block_number() == 100


def test_candle_feed_fork_last_block():
    """Make sure if the last block forks we do not get confused.

    Chain reorganisation happens at the last block we have read.
    """

    mock_chain = MockChainAndReorganisationMonitor(block_duration_seconds=12)
    mock_chain.produce_blocks(100)
    timeframe = Timeframe("1min")

    feed = SyntheticTradeFeed(
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
    assert candle_feed.get_last_block_number() == 100
    flat = candle_feed.candle_df.reset_index(drop=True)
    assert len(flat) == 21

    # Add 1 block
    mock_chain.produce_blocks(1)
    delta = feed.perform_duty_cycle()
    candle_feed.apply_delta(delta)
    assert candle_feed.get_last_block_number() == 101
    flat = candle_feed.candle_df.reset_index(drop=True)
    assert len(flat) == 21

    # Reorg the last block
    mock_chain.produce_fork(101, fork_marker="0x7777")
    delta = feed.perform_duty_cycle()
    candle_feed.apply_delta(delta)
    assert delta.reorg_detected
    assert delta.start_block == 100
    assert delta.unadjusted_start_block == 101
    assert delta.end_block == 101
    assert candle_feed.get_last_block_number() == 101
    flat = candle_feed.candle_df.reset_index(drop=True)
    assert len(flat) == 21


def test_candle_feed_two_pairs():
    """Make sure if the last block forks we do not get confused with two different pairs."""

    mock_chain = MockChainAndReorganisationMonitor(block_duration_seconds=12)
    mock_chain.produce_blocks(100)
    timeframe = Timeframe("1min")

    pairs = ["ETH-USD", "AAVE-ETH"]

    feed = SyntheticTradeFeed(
        pairs,
        {
            "ETH-USD": TrustedStablecoinOracle(),
            "AAVE-ETH": FixedPriceOracle(1600),
        },
        mock_chain,
        timeframe=timeframe,
        min_amount=-50,
        max_amount=50,
    )

    candle_feed =  CandleFeed(
        pairs,
        timeframe=timeframe,
    )

    delta = feed.backfill_buffer(100, None)
    candle_feed.apply_delta(delta)
    assert candle_feed.get_last_block_number() == 100
    flat = candle_feed.candle_df.reset_index(drop=True)
    assert len(flat) == 42

    # Add 1 block
    mock_chain.produce_blocks(1)
    delta = feed.perform_duty_cycle()
    candle_feed.apply_delta(delta)
    assert candle_feed.get_last_block_number() == 101
    flat = candle_feed.candle_df.reset_index(drop=True)
    assert len(flat) == 42

    # Reorg the last block
    mock_chain.produce_fork(101, fork_marker="0x7777")
    delta = feed.perform_duty_cycle()
    candle_feed.apply_delta(delta)
    assert delta.reorg_detected
    assert delta.start_block == 100
    assert delta.unadjusted_start_block == 101
    assert delta.end_block == 101
    assert candle_feed.get_last_block_number() == 101
    flat = candle_feed.candle_df.reset_index(drop=True)
    assert len(flat) == 42

    # Build on the top of the fork
    mock_chain.produce_blocks(100)
    delta = feed.perform_duty_cycle()
    candle_feed.apply_delta(delta)
    assert not delta.reorg_detected
    assert delta.start_block == 100
    assert delta.unadjusted_start_block == 102
    assert delta.end_block == 201
    assert candle_feed.get_last_block_number() == 201
    flat = candle_feed.candle_df.reset_index(drop=True)
    assert len(flat) == 82
