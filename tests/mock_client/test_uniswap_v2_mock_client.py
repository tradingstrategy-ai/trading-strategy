import pytest
from web3.middleware import gas_price_strategy_middleware, buffered_gas_estimate_middleware
from web3.providers.eth_tester.middleware import default_transaction_fields_middleware, ethereum_tester_middleware

from eth_defi.token import fetch_erc20_details, create_token
from eth_defi.uniswap_v2.deployment import UniswapV2Deployment, deploy_uniswap_v2_like, deploy_trading_pair
from eth_typing import HexAddress
from web3 import Web3, EthereumTesterProvider
from web3.contract import Contract

from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.testing.uniswap_v2_mock_client import UniswapV2MockClient


@pytest.fixture
def tester_provider():
    provider = EthereumTesterProvider()
    # Remove web3 6.0 AttributedDict middleware
    provider.middlewares = (
        default_transaction_fields_middleware,
        ethereum_tester_middleware,
    )
    return provider


@pytest.fixture
def eth_tester(tester_provider):
    return tester_provider.ethereum_tester


@pytest.fixture
def web3(tester_provider):
    """Set up a local unit testing blockchain."""
    web3 = Web3(tester_provider)
    # # Remove web3 6.0 AttributedDict middleware
    web3.middleware_onion.clear()
    web3.middleware_onion.add(gas_price_strategy_middleware)
    web3.middleware_onion.add(buffered_gas_estimate_middleware)
    return web3


@pytest.fixture()
def deployer(web3) -> HexAddress:
    """Deployer account.

    - This account will deploy all smart contracts

    - Starts with 10,000 ETH
    """
    return web3.eth.accounts[0]


@pytest.fixture()
def uniswap_v2(web3: Web3, deployer: HexAddress) -> UniswapV2Deployment:
    """Deploy Uniswap, WETH token."""
    assert web3.eth.get_balance(deployer) > 0
    deployment = deploy_uniswap_v2_like(web3, deployer, give_weth=500)  # Will also deploy WETH9 and give the deployer this many WETH tokens
    return deployment


@pytest.fixture()
def user_1(web3) -> HexAddress:
    """User account.

    Do some account allocation for tests.
    """
    return web3.eth.accounts[1]


@pytest.fixture()
def user_2(web3) -> HexAddress:
    """User account.

    Do some account allocation for tests.
    """
    return web3.eth.accounts[2]


@pytest.fixture()
def user_3(web3) -> HexAddress:
    """User account.

    Do some account allocation for tests.
    """
    return web3.eth.accounts[3]


@pytest.fixture
def weth(uniswap_v2) -> Contract:
    return uniswap_v2.weth


@pytest.fixture()
def usdc(web3, deployer) -> Contract:
    """Mock USDC token.

    All initial $100M goes to `deployer`
    """
    token = create_token(web3, deployer, "USD Coin", "USDC", 100_000_000 * 10**6, decimals=6)
    return token


@pytest.fixture
def aave(web3, deployer: HexAddress) -> Contract:
    """Create AAVE with 10M supply."""
    token = create_token(web3, deployer, "Fake Aave coin", "AAVE", 10_000_000 * 10**18, 18)
    return token


@pytest.fixture()
def weth_usdc_uniswap_pair(web3, deployer, uniswap_v2, usdc, weth) -> HexAddress:
    """Create Uniswap v2 pool for WETH-USDC.

    - Add 200k initial liquidity at 1600 ETH/USDC
    """

    deposit = 200_000  # USDC
    price = 1600

    pair = deploy_trading_pair(
        web3,
        deployer,
        uniswap_v2,
        usdc,
        weth,
        deposit * 10**6,
        (deposit // price) * 10**18,
    )

    return pair


@pytest.fixture
def aave_usdc_uniswap_trading_pair(web3, deployer, uniswap_v2, aave, usdc) -> HexAddress:
    """AAVE-USDC pool with 200k liquidity."""
    pair_address = deploy_trading_pair(
        web3,
        deployer,
        uniswap_v2,
        aave,
        usdc,
        1000 * 10**18,  # 1000 AAVE liquidity
        200_000 * 10**6,  # 200k USDC liquidity
    )
    return pair_address


def test_uniswap_v2_mock_client(
    web3: Web3,
    uniswap_v2: UniswapV2Deployment,
    usdc: Contract,
    weth_usdc_uniswap_pair: str,
    aave_usdc_uniswap_trading_pair: str,
):
    """Test pair data iteration."""

    client = UniswapV2MockClient(
        web3,
        uniswap_v2.factory.address,
        uniswap_v2.router.address,
        uniswap_v2.init_code_hash,
    )
    client.initialise_mock_data()

    exchange_universe = client.fetch_exchange_universe()
    assert len(exchange_universe.exchanges) == 1

    exchange = exchange_universe.exchanges[1]
    assert exchange.exchange_slug == "UniswapV2MockClient"

    pairs_df = client.fetch_pair_universe().to_pandas()
    pair_universe = PandasPairUniverse(pairs_df)

    assert pair_universe.get_count() == 2

    weth_usdc = pair_universe.get_pair_by_id(1)
    assert weth_usdc.base_token_symbol == "WETH"
    assert weth_usdc.quote_token_symbol == "USDC"
    assert weth_usdc.fee == 30

    aave_usdc = pair_universe.get_pair_by_id(2)
    assert aave_usdc.base_token_symbol == "AAVE"
    assert aave_usdc.quote_token_symbol == "USDC"

    assert client.get_default_quote_token_address() == usdc.address.lower()
