
"""Client lending dataset download and integrity tests"""

import logging
from _decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from tradingstrategy.chain import ChainId
from tradingstrategy.pair import filter_for_chain, filter_for_quote_tokens, StablecoinFilteringMode, filter_for_stablecoins, PandasPairUniverse, \
    filter_for_base_tokens
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.client import Client
from tradingstrategy.lending import LendingCandleType, LendingReserveUniverse, LendingProtocolType, UnknownLendingReserve, LendingReserve, \
    LendingCandleUniverse, NoLendingData
from tradingstrategy.utils.time import ZERO_TIMEDELTA

logger = logging.getLogger(__name__)


def test_client_download_lending_reserves_all_time(client: Client, cache_path: str):
    """Download all available data on lending protocol reserves."""
    df = client.fetch_lending_reserves_all_time()
    # Check we cached the file correctly
    assert Path(f"{cache_path}/lending-reserves-all.parquet").exists()
    assert len(df) > 100


def test_client_download_lending_reserve_universe(client: Client, cache_path: str):
    universe = client.fetch_lending_reserve_universe()
    # Check we cached the file correctly
    assert Path(f"{cache_path}/lending-reserve-universe.json").exists()
    assert len(universe.reserves) > 50


def test_fetch_lending_reserve_info(client: Client):
    universe = client.fetch_lending_reserve_universe()

    reserve = universe.get_reserve_by_id(2)
    assert reserve.reserve_id
    assert reserve.chain_id == 137
    assert reserve.asset_symbol == "LINK"
    assert reserve.asset_address == "0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39"
    assert reserve.asset_decimals == 18
    assert reserve.atoken_symbol == "aPolLINK"
    assert reserve.atoken_address == "0x191c10aa4af7c30e871e70c95db0e4eb77237530"
    assert reserve.atoken_decimals == 18
    assert reserve.vtoken_symbol == "variableDebtPolLINK"
    assert reserve.vtoken_address == "0x953a573793604af8d41f306feb8274190db4ae0e"
    assert reserve.vtoken_decimals == 18

    reserve = universe.get_by_chain_and_symbol(ChainId.ethereum, "USDC")
    assert reserve.reserve_id
    assert reserve.chain_id == 1
    assert reserve.asset_symbol == "USDC"
    assert reserve.asset_address == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    assert reserve.asset_decimals == 6
    assert reserve.atoken_symbol == "aEthUSDC"
    assert reserve.atoken_address == "0x98c23e9d8f34fefb1b7bd6a91b7ff122f4e16f5c"
    assert reserve.atoken_decimals == 6
    assert 0 < reserve.additional_details.ltv < 1
    assert 0 < reserve.additional_details.liquidation_threshold < 1


def test_client_fetch_lending_candles(client: Client, cache_path: str):
    df = client.fetch_lending_candles_by_reserve_id(1, TimeBucket.h1)
    assert len(df) > 50
    assert len(list(Path(cache_path).glob("variable-borrow-apr-jsonl-1h-between-any-and-any-*.parquet"))) == 1

    df = client.fetch_lending_candles_by_reserve_id(3, TimeBucket.d1, candle_type=LendingCandleType.supply_apr)
    assert len(df) > 50
    assert len(list(Path(cache_path).glob("supply-apr-jsonl-1d-between-any-and-any-*.parquet"))) == 1


def test_resolve_lending_reserve(persistent_test_client):
    """Look up lending reserve by a human description"""
    client = persistent_test_client
    universe = client.fetch_lending_reserve_universe()

    usdt_reserve = universe.resolve_lending_reserve(
        (
            ChainId.polygon,
            LendingProtocolType.aave_v3,
            "USDT",
        )
    )
    assert isinstance(usdt_reserve, LendingReserve)
    assert usdt_reserve.asset_address == '0xc2132d05d31c914a87c6611c10748aeb04b58e8f'
    assert usdt_reserve.asset_symbol == "USDT"
    assert usdt_reserve.asset_name == '(PoS) Tether USD'
    assert usdt_reserve.asset_decimals == 6
    assert usdt_reserve.atoken_symbol == "aPolUSDT"
    assert usdt_reserve.atoken_decimals == 6
    assert usdt_reserve.vtoken_symbol == "variableDebtPolUSDT"
    assert usdt_reserve.vtoken_decimals == 6
    assert 0 < usdt_reserve.additional_details.ltv < 1
    assert 0 < usdt_reserve.additional_details.liquidation_threshold < 1

    weth_reserve = universe.resolve_lending_reserve(
        (
            ChainId.ethereum,
            LendingProtocolType.aave_v2,
            "WETH",
        )
    )
    assert isinstance(weth_reserve, LendingReserve)
    assert weth_reserve.asset_address == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    assert weth_reserve.asset_symbol == "WETH"
    assert weth_reserve.asset_name == "Wrapped Ether"
    assert weth_reserve.asset_decimals == 18
    assert weth_reserve.atoken_symbol == "aWETH"
    assert weth_reserve.atoken_decimals == 18
    assert weth_reserve.vtoken_symbol == "variableDebtWETH"
    assert weth_reserve.vtoken_decimals == 18
    # we don't have this info for v2 yet
    # assert 0 < weth_reserve.additional_details.ltv < 1
    # assert 0 < weth_reserve.additional_details.liquidation_threshold < 1


def test_lending_reserve_equal(persistent_test_client):
    """Check for lending reserve equality"""
    client = persistent_test_client

    universe = client.fetch_lending_reserve_universe()
    universe2 = client.fetch_lending_reserve_universe()

    usdt_reserve = universe.resolve_lending_reserve(
        (ChainId.polygon,
        LendingProtocolType.aave_v3,
        "USDT")
    )
    usdt_reserve2 = universe2.resolve_lending_reserve(
        (ChainId.polygon,
         LendingProtocolType.aave_v3,
         "USDT")
    )
    assert usdt_reserve == usdt_reserve2

    usdc_reserve = universe.resolve_lending_reserve(
        (ChainId.polygon,
         LendingProtocolType.aave_v3,
         "USDC")
    )
    assert usdt_reserve != usdc_reserve


def test_resolve_lending_reserve_unknown(persistent_test_client):
    """Look up lending reserve by a human description, but typo it out"""
    client = persistent_test_client
    universe = client.fetch_lending_reserve_universe()

    with pytest.raises(UnknownLendingReserve):
        universe.resolve_lending_reserve((ChainId.polygon, LendingProtocolType.aave_v3, "XXX"))


def test_limit_lending_reserve_universe(persistent_test_client):
    """Reduce lending universe in size."""
    client = persistent_test_client
    universe = client.fetch_lending_reserve_universe()

    limited_universe = universe.limit([
        (ChainId.polygon, LendingProtocolType.aave_v3, "USDT"),
        (ChainId.polygon, LendingProtocolType.aave_v3, "USDC"),
        (ChainId.ethereum, LendingProtocolType.aave_v2, "USDT"),
    ])

    assert limited_universe.get_count() == 3


def test_client_fetch_lending_candles_for_lending_universe(persistent_test_client: Client):
    """Load lending candles for several reserves and create a lending candle universe.

    - Load both variable borrow and supply rates

    - Load for USDT and USDC on Polygon

    - Load for historical 1 month

    - Use two different accessor methods to read data
    """

    client= persistent_test_client
    universe = client.fetch_lending_reserve_universe()

    usdt_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDT")
    usdc_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDC.e")

    limited_universe = universe.limit([usdt_desc, usdc_desc])

    usdt_reserve = limited_universe.resolve_lending_reserve(usdt_desc)
    usdc_reserve = limited_universe.resolve_lending_reserve(usdc_desc)

    lending_candle_type_map = client.fetch_lending_candles_for_universe(
        limited_universe,
        TimeBucket.d1,
        start_time=pd.Timestamp("2023-01-01"),
        end_time=pd.Timestamp("2023-02-01"),
    )

    universe = LendingCandleUniverse(lending_candle_type_map, universe)

    # Read all data for a single reserve
    usdc_variable_borrow = universe.variable_borrow_apr.get_samples_by_pair(usdc_reserve.reserve_id)

    #            reserve_id      open      high       low     close  timestamp
    # timestamp
    # 2023-01-01           3  1.836242  1.839224  1.780513  1.780513 2023-01-01

    assert usdc_variable_borrow["open"][pd.Timestamp("2023-01-01")] == pytest.approx(1.8362419852748817)
    assert usdc_variable_borrow["close"][pd.Timestamp("2023-01-01")] == pytest.approx(1.780513)

    # Read data for multiple reserves for a time range

    #             reserve_id      open      high       low     close  timestamp
    # timestamp
    # 2023-01-05           6  2.814886  2.929328  2.813202  2.867843 2023-01-05
    # 2023-01-06           6  2.868013  2.928622  2.829608  2.866523 2023-01-06

    start = pd.Timestamp("2023-01-05")
    end = pd.Timestamp("2023-01-06")
    iterator = universe.supply_apr.iterate_samples_by_pair_range(start, end)
    for reserve_id, supply_apr in iterator:
        # Read supply apr only for USDT
        if reserve_id == usdt_reserve.reserve_id:
            assert len(supply_apr) == 2  # 2 days
            assert supply_apr["open"][pd.Timestamp("2023-01-05")] == pytest.approx(2.060042143388868)
            assert supply_apr["close"][pd.Timestamp("2023-01-06")] == pytest.approx(2.1192639317338413)


def test_get_rates_by_reserve(persistent_test_client: Client):
    """Get all rates for a single reserve by description."""

    client = persistent_test_client
    universe = client.fetch_lending_reserve_universe()

    usdt_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDT")
    usdc_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDC.e")

    limited_universe = universe.limit([usdt_desc, usdc_desc])

    lending_candle_type_map = client.fetch_lending_candles_for_universe(
        limited_universe,
        TimeBucket.d1,
        start_time=pd.Timestamp("2023-01-01"),
        end_time=pd.Timestamp("2023-02-01"),
    )

    universe = LendingCandleUniverse(lending_candle_type_map, universe)
    usdc_variable_borrow = universe.variable_borrow_apr.get_rates_by_reserve(
        usdc_desc,
    )

    # See above
    assert usdc_variable_borrow["open"][pd.Timestamp("2023-01-01")] == pytest.approx(1.836242)
    assert usdc_variable_borrow["close"][pd.Timestamp("2023-01-01")] == pytest.approx(1.780513)


def test_lending_universe_construction(persistent_test_client: Client):
    """Construct lending universe with various filters."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()

    quote_tokens = {
        "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",  # USDC polygon
        "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",  # USDT polygon
    }

    pairs_df = client.fetch_pair_universe().to_pandas()

    # Find out all volatile pairs traded against USDC and USDT on Polygon
    pairs_df = filter_for_chain(pairs_df, ChainId.polygon)
    pairs_df = filter_for_stablecoins(pairs_df, StablecoinFilteringMode.only_volatile_pairs)
    pairs_df = filter_for_quote_tokens(pairs_df, quote_tokens)

    # Create lending universe and trading universe with the cross section of
    # - Available assets in the lending protocols
    # - Asset we can trade
    lending_reserves = client.fetch_lending_reserve_universe()
    lending_only_pairs_df = filter_for_base_tokens(pairs_df, lending_reserves.get_asset_addresses())

    pair_universe = PandasPairUniverse(lending_only_pairs_df, exchange_universe=exchange_universe)

    # Lending reserves have around ~320 individual trading pairs on Polygon across different DEXes
    assert 1 < pair_universe.get_count() < 1_000

    eth_usdc = pair_universe.get_pair_by_human_description((ChainId.polygon, "uniswap-v3", "WETH", "USDC"))
    matic_usdc = pair_universe.get_pair_by_human_description((ChainId.polygon, "uniswap-v3", "WMATIC", "USDC"))

    # Random token not in the Aave v3 supported reserves
    full_pair_universe =  PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)
    bob_usdc = full_pair_universe.get_pair_by_human_description((ChainId.polygon, "uniswap-v3", "BOB", "USDC"))

    assert lending_reserves.can_leverage(eth_usdc.get_base_token())
    assert lending_reserves.can_leverage(matic_usdc.get_base_token())
    assert not lending_reserves.can_leverage(bob_usdc.get_base_token())


def test_get_single_rate(persistent_test_client: Client):
    """Get a single historical rate."""

    client = persistent_test_client
    lending_reserves = client.fetch_lending_reserve_universe()

    usdt_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDT")
    usdc_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDC.e")

    lending_reserves = lending_reserves.limit([usdt_desc, usdc_desc])

    lending_candle_type_map = client.fetch_lending_candles_for_universe(
        lending_reserves,
        TimeBucket.d1,
        start_time=pd.Timestamp("2023-01-01"),
        end_time=pd.Timestamp("2023-02-01"),
    )

    lending_candles = LendingCandleUniverse(lending_candle_type_map, lending_reserves)

    usdc_reserve = lending_reserves.resolve_lending_reserve(usdc_desc)

    rate, lag = lending_candles.variable_borrow_apr.get_single_rate(
        usdc_reserve,
        pd.Timestamp("2023-01-01"),
        data_lag_tolerance=pd.Timedelta(days=1),
        kind="open",
    )
    assert rate == pytest.approx(1.836242)
    assert lag == ZERO_TIMEDELTA


@pytest.mark.parametrize(
    "start, end, interest_type, expected_multiplier",
    (
        (
            pd.Timestamp("2022-09-01"),
            pd.Timestamp("2023-09-01"),
            "variable_borrow_apr",
            Decimal("1.028597760665127969909038441"),
        ),
        (
            pd.Timestamp("2022-09-01"),
            pd.Timestamp("2023-09-01"),
            "supply_apr",
            Decimal("1.017786465640168974688961612"),
        ),
        (
            pd.Timestamp("2023-08-03 10:00:00"),
            pd.Timestamp("2023-08-03 10:20:00"),
            "supply_apr",
            Decimal("1.000001990000776199338106284"),
        ),
    ),
)
def test_get_estimate_interest(
    persistent_test_client: Client, 
    start, 
    end, 
    interest_type, 
    expected_multiplier,
):
    """Estimate how much profit we make with USDC credit."""

    client = persistent_test_client

    usdc_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDC.e")
    lending_reserves = client.fetch_lending_reserve_universe().limit([usdc_desc])

    lending_candle_type_map = client.fetch_lending_candles_for_universe(
        lending_reserves,
        TimeBucket.d1,
        start_time=pd.Timestamp("2022-09-01"),
        end_time=pd.Timestamp("2023-09-01"),
    )
    lending_candles = LendingCandleUniverse(lending_candle_type_map, lending_reserves)

    interest_multiplier = getattr(lending_candles, interest_type).estimate_accrued_interest(
        usdc_desc,
        start=start,
        end=end,
    )
    assert interest_multiplier == pytest.approx(expected_multiplier)


def test_estimate_interest_bad_timeframe(persistent_test_client: Client):
    """Try estimate interest when we have no data."""

    client = persistent_test_client
    lending_reserves = client.fetch_lending_reserve_universe()

    usdc_desc = (ChainId.polygon, LendingProtocolType.aave_v3, "USDC.e")

    lending_reserves = lending_reserves.limit([usdc_desc])

    lending_candle_type_map = client.fetch_lending_candles_for_universe(
        lending_reserves,
        TimeBucket.d1,
        start_time=pd.Timestamp("2022-09-01"),
        end_time=pd.Timestamp("2023-09-01"),
    )
    lending_candles = LendingCandleUniverse(lending_candle_type_map, lending_reserves)

    # Ask for non-existing period
    with pytest.raises(NoLendingData):
        lending_candles.variable_borrow_apr.estimate_accrued_interest(
            usdc_desc,
            start=pd.Timestamp("2020-01-01"),
            end=pd.Timestamp("2020-01-02"),
        )
