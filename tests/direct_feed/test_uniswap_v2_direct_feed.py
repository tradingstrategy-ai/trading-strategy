"""Uniswap v2 data collection."""
import logging

import pytest
from eth_typing import HexAddress
from web3 import EthereumTesterProvider, Web3
from web3.middleware import attrdict_middleware
from web3.providers.eth_tester.middleware import default_transaction_fields_middleware, ethereum_tester_middleware

from eth_defi.chain import install_chain_middleware
from eth_defi.event_reader.fast_json_rpc import patch_web3
from eth_defi.event_reader.web3factory import SimpleWeb3Factory
from eth_defi.price_oracle.oracle import TrustedStablecoinOracle
from eth_defi.token import create_token, TokenDetails, fetch_erc20_details
from eth_defi.uniswap_v2.deployment import (
    UniswapV2Deployment,
    deploy_uniswap_v2_like,
)
from eth_defi.uniswap_v2.pair import fetch_pair_details
from eth_defi.uniswap_v2.synthetic_data import generate_fake_uniswap_v2_data
from tradingstrategy.direct_feed.candle_feed import CandleFeed
from eth_defi.event_reader.reorganisation_monitor import JSONRPCReorganisationMonitor
from tradingstrategy.direct_feed.timeframe import Timeframe
from tradingstrategy.direct_feed.trade_feed import Trade
from tradingstrategy.direct_feed.uniswap_v2 import UniswapV2TradeFeed
from tradingstrategy.direct_feed.warn import disable_pandas_warnings


@pytest.fixture
def tester_provider():
    # https://web3py.readthedocs.io/en/stable/examples.html#contract-unit-tests-in-python
    provider = EthereumTesterProvider()

    # Web3 6.0
    # TypeError: 'AttributeDict' object does not support item assignment
    provider.middlewares = (
    #    attrdict_middleware,
        default_transaction_fields_middleware,
        ethereum_tester_middleware,
    )
    return provider


@pytest.fixture
def eth_tester(tester_provider):
    # https://web3py.readthedocs.io/en/stable/examples.html#contract-unit-tests-in-python
    return tester_provider.ethereum_tester


@pytest.fixture
def web3(tester_provider):
    """Set up a local unit testing blockchain."""
    # https://web3py.readthedocs.io/en/stable/examples.html#contract-unit-tests-in-python
    web3 = Web3(tester_provider)
    install_chain_middleware(web3)
    return web3


@pytest.fixture()
def deployer(web3) -> str:
    """Deploy account.

    Do some account allocation for tests.
    """
    return web3.eth.accounts[0]


@pytest.fixture()
def uniswap_v2(web3, deployer) -> UniswapV2Deployment:
    """Uniswap v2 deployment."""
    return deploy_uniswap_v2_like(web3, deployer)


@pytest.fixture()
def usdc(web3, deployer) -> TokenDetails:
    """Mock USDC token."""
    token = create_token(web3, deployer, "USD Coin", "USDC", 100_000_000 * 10 ** 6, decimals=6)
    return fetch_erc20_details(web3, token.address)


@pytest.fixture()
def weth(uniswap_v2) -> TokenDetails:
    """Mock WETH token."""
    return fetch_erc20_details(uniswap_v2.web3, uniswap_v2.weth.address)


def test_uniswap_v2_direct_feed(
        web3,
        tester_provider,
        uniswap_v2: UniswapV2Deployment,
        deployer: HexAddress,
        weth: TokenDetails,
        usdc: TokenDetails):
    """Read random ETH-USD trades from EthereumTester blockchain."""

    disable_pandas_warnings()

    # Don't spam DEBUG level
    # when testing the code
    bad_loggers = [
        logging.getLogger("web3.RequestManager"),
        logging.getLogger("eth.vm.base.VM.LondonVM"),
        logging.getLogger("eth.chain.chain.Chain"),
    ]
    for logger in bad_loggers:
        logger.setLevel(logging.WARNING)

    synthetic_data_stats = generate_fake_uniswap_v2_data(
        uniswap_v2,
        deployer,
        weth,
        usdc,
        base_liquidity=100 * 10 ** 18,  # 100 ETH liquidity
        quote_liquidity=1600 * 100 * 10 ** 6,  # 170,000 USDC liquidity,
        number_of_blocks=20,
        trades_per_block=1,
    )

    # Check all transactiosn got mined
    for tx_hash in synthetic_data_stats["tx_hashes"]:
        receipt = web3.eth.get_transaction_receipt(tx_hash)
        assert receipt

    pair_address: str = synthetic_data_stats["pair_address"]

    # Depending on the randomness, contracts might be deployed in different order
    reverse_token_order = int(weth.address, 16) > int(usdc.address, 16)

    pair = fetch_pair_details(web3, pair_address, reverse_token_order)

    pair_ids = [pair.address]
    pairs = [pair]

    oracles = {
        pair.checksum_free_address: TrustedStablecoinOracle(),
    }

    timeframe = Timeframe("1min")

    candle_feed = CandleFeed(
        pair_ids,
        timeframe=timeframe,
    )

    # Prepare the web3 connection for the scan
    web3_patched = Web3(tester_provider)
    patch_web3(web3_patched)
    web3_patched.middleware_onion.clear()

    reorg_mon = JSONRPCReorganisationMonitor(web3_patched)

    web3_factory = SimpleWeb3Factory(web3_patched)

    trade_feed = UniswapV2TradeFeed(
        pairs,
        web3_factory,
        oracles,
        reorg_mon,
        threads=1,
        chunk_size=3,
        timeframe=timeframe,
    )

    delta = trade_feed.perform_duty_cycle()
    trades = delta.trades

    candle_feed.apply_delta(delta)

    # Check that trades match the happened event range
    assert trades.iloc[0].block_number == synthetic_data_stats["first_block"]
    assert trades.iloc[-1].block_number == synthetic_data_stats["last_block"]

    # Check that we processed all Swap events
    buys = Trade.filter_buys(delta.trades)
    sells = Trade.filter_sells(delta.trades)
    assert len(buys) == synthetic_data_stats["buys"]
    assert len(sells) == synthetic_data_stats["sells"]

    # We start random walk in a forest at ~1600 and should stay around the range
    for t in trades.itertuples():
        assert t.price > 1400
        assert t.price < 1700
        assert t.amount > -1000
        assert t.amount < 1000

    # Do some more trades and see
    # we can read incremental updates.
    # Note that we need to have unpatched web3 instance heere
    round_two = generate_fake_uniswap_v2_data(
        uniswap_v2,
        deployer,
        weth,
        usdc,
        pair_address=synthetic_data_stats["pair_address"],
        number_of_blocks=5,
        trades_per_block=1,
    )
    delta = trade_feed.perform_duty_cycle()
    trades = delta.trades
    # Note that trades are rounded down to the nearest candle
    assert trades.iloc[0].block_number <= round_two["first_block"]
    assert trades.iloc[-1].block_number == round_two["last_block"]
    buys = Trade.filter_buys(delta.trades)
    sells = Trade.filter_sells(delta.trades)
    assert len(buys) >= round_two["buys"]
    assert len(sells) >= round_two["sells"]

    candle_feed.apply_delta(delta)

    # Read trades back
    candles = candle_feed.get_candles_by_pair(pair.address.lower())
    assert len(candles) >= 2
    assert float(candles.iloc[-1]["close"]) == pytest.approx(1612.715061450271610294202054)
