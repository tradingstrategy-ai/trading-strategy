"""Trading data availabiltiy tests."""
import datetime

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket


def test_trading_data_availability(persistent_test_client: Client):
    """Load trading data availability from the live oracle for a single pair"""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD")],
        )

    pair = pair_universe.get_single()

    pairs_availability = client.fetch_trading_data_availability({pair.pair_id}, TimeBucket.m15)
    assert len(pairs_availability) == 1
    avail = pairs_availability[pair.pair_id]

    # Check values are properly deserialised
    assert avail["chain_id"] == ChainId.bsc
    assert avail["pair_address"] == "0x58f876857a02d6762e0101bb5c46a8c1ed44dc16"
    assert avail["last_candle_at"] > datetime.datetime(1970, 1, 1)
    assert avail["last_supposed_candle_at"] > datetime.datetime(1970, 1, 1)
    assert avail["last_trade_at"] > datetime.datetime(1970, 1, 1)


def test_trading_data_availability_uniswap_v3(persistent_test_client: Client):
    """Load trading data availability from the live oracle for a single pair"""

    client = persistent_test_client
    pairs_df = client.fetch_pair_universe().to_pandas()

    # Create filtered exchange and pair data
    pair_universe = PandasPairUniverse.create_pair_universe(
        pairs_df,
        [(ChainId.ethereum, "uniswap-v3", "WETH", "USDC", 0.0005)],
    )

    pair = pair_universe.get_single()

    pairs_availability = client.fetch_trading_data_availability({pair.pair_id}, TimeBucket.m15)
    assert len(pairs_availability) == 1
    avail = pairs_availability[pair.pair_id]

    # Check values are properly deserialised
    assert avail["chain_id"] == ChainId.ethereum
    assert avail["pair_address"] == "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
    assert avail["last_candle_at"] > datetime.datetime(1970, 1, 1)
    assert avail["last_supposed_candle_at"] > datetime.datetime(1970, 1, 1)
    assert avail["last_trade_at"] > datetime.datetime(1970, 1, 1)
