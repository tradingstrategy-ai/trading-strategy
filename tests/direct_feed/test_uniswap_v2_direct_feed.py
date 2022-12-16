"""Uniswap v2 data collection."""

import secrets
from decimal import Decimal

import pytest
from eth_account import Account
from eth_account.signers.local import LocalAccount
from hexbytes import HexBytes
from web3 import EthereumTesterProvider, Web3

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
from tradingstrategy.direct_feed.reorg_mon import JSONRPCReorganisationMonitor
from tradingstrategy.direct_feed.trade_feed import Trade
from tradingstrategy.direct_feed.uniswap_v2 import UniswapV2TradeFeed


@pytest.fixture
def tester_provider():
    # https://web3py.readthedocs.io/en/stable/examples.html#contract-unit-tests-in-python
    return EthereumTesterProvider()


@pytest.fixture
def eth_tester(tester_provider):
    # https://web3py.readthedocs.io/en/stable/examples.html#contract-unit-tests-in-python
    return tester_provider.ethereum_tester


@pytest.fixture
def web3(tester_provider):
    """Set up a local unit testing blockchain."""
    # https://web3py.readthedocs.io/en/stable/examples.html#contract-unit-tests-in-python
    return Web3(tester_provider)


@pytest.fixture()
def deployer(web3) -> str:
    """Deploy account.

    Do some account allocation for tests.
    """
    return web3.eth.accounts[0]


@pytest.fixture()
def user_1(web3) -> str:
    """User account.

    Do some account allocation for tests.
    """
    return web3.eth.accounts[1]


@pytest.fixture()
def user_2(web3) -> str:
    """User account.

    Do some account allocation for tests.
    """
    return web3.eth.accounts[2]


@pytest.fixture()
def hot_wallet_private_key() -> HexBytes:
    """Generate a private key"""
    return HexBytes(secrets.token_bytes(32))


@pytest.fixture()
def hot_wallet(eth_tester, hot_wallet_private_key) -> LocalAccount:
    """User account.

    Do some account allocation for tests.
    '"""
    # also add to eth_tester so we can use transact() directly
    eth_tester.add_account(hot_wallet_private_key.hex())
    return Account.from_key(hot_wallet_private_key)


@pytest.fixture()
def uniswap_v2(web3, deployer) -> UniswapV2Deployment:
    """Uniswap v2 deployment."""
    return deploy_uniswap_v2_like(web3, deployer)


@pytest.fixture()
def usdc(web3, deployer) -> TokenDetails:
    """Mock USDC token."""
    token = create_token(web3, deployer, "USD Coin", "USDC", 100_000_000 * 10**6, decimals=6)
    return fetch_erc20_details(web3, token.address)


@pytest.fixture()
def weth(uniswap_v2) -> TokenDetails:
    """Mock WETH token."""
    return fetch_erc20_details(uniswap_v2.web3, uniswap_v2.weth.address)


def test_uniswap_v2_direct_feed(web3, uniswap_v2, deployer, weth, usdc):
    """Read random ETH-USD trades from EthereumTester blockchain."""

    synthetic_data_stats = generate_fake_uniswap_v2_data(
        uniswap_v2,
        deployer,
        weth,
        usdc,
        base_liquidity=100 * 10**18,  # 100 ETH liquidity
        quote_liquidity=1600 * 100 * 10**6,  # 170,000 USDC liquidity,
        number_of_blocks=10,
    )

    pair_address: str = synthetic_data_stats["pair_address"]

    # Depending on the randomness, contracts might be deployed in different order
    reverse_token_order = int(weth.address, 16) > int(usdc.address, 16)

    pair = fetch_pair_details(web3, pair_address, reverse_token_order)

    pairs = [pair]

    oracles = {
        pair: TrustedStablecoinOracle(),
    }

    # Prepare the web3 connection for the scan
    patch_web3(web3)
    web3.middleware_onion.clear()

    reorg_mon = JSONRPCReorganisationMonitor(web3)

    web3_factory = SimpleWeb3Factory(web3)

    trade_feed = UniswapV2TradeFeed(
        pairs,
        web3_factory,
        oracles,
        reorg_mon,
        threads=1,
        chunk_size=3,
    )

    delta = trade_feed.perform_duty_cycle()
    trades = delta.trades

    # Check that trades match the happened event range
    import ipdb ; ipdb.set_trace()
    assert trades.iloc[0].block_number == synthetic_data_stats["first_block"]
    assert trades.iloc[-1].block_number == synthetic_data_stats["last_block"]

    # Check that we processed all Swap events
    buys = Trade.filter_buys(delta.trades)
    sells = Trade.filter_sells(delta.trades)
    assert len(buys) == synthetic_data_stats["buys"]
    assert len(sells) == synthetic_data_stats["sells"]

    import ipdb ; ipdb.set_trace()


