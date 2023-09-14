"""Client implementation that only uses Uniswap v2 on-chain data to generate datasets."""
import logging
from types import NoneType
from typing import Tuple, cast

from pyarrow import Table

from web3 import Web3, HTTPProvider
from eth_defi.abi import get_contract
from eth_defi.event_reader.conversion import decode_data, convert_uint256_bytes_to_address, convert_int256_bytes_to_int, convert_uint256_hex_string_to_address
from eth_defi.event_reader.filter import Filter
from eth_defi.event_reader.logresult import LogResult
from eth_defi.event_reader.reader import read_events
from eth_defi.token import fetch_erc20_details
from eth_typing import HexAddress, HexStr

from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import Exchange, ExchangeType, ExchangeUniverse
from tradingstrategy.pair import DEXPair, PandasPairUniverse
from tradingstrategy.stablecoin import is_stablecoin_like
from tradingstrategy.testing.mock_client import MockClient


logger = logging.getLogger(__name__)


class UniswapV2MockClient(MockClient):
    """A mock client that reads data from a single Uniswap v2 exchange directly from the chain.

    Designed to run tests against test EVM backends where we cannot generate
    proper test data because of the backends being temporary. This way we can skip the ETL
    step and pretend that the data is just there, but still have meaningful interaction
    with trading strategies with pairs and trade execution and such.

    Currently supported

    - Exchanges

    - Pairs data

    Any data is read from the chain on construction, then cached for subsequent fetch calls.

    .. warning::

        This client is not suitable to iterate real on-chain data.
        Due to high amount of real pairs deployed, you will need to wait
        several hours for :py:methd:`read_onchain_data` to complete.

    """

    def __init__(self,
                 web3: Web3,
                factory_address: HexAddress | str,
                router_address: HexAddress | str,
                init_code_hash: HexStr | str | NoneType = None,
                fee: float = 0.0030,
                 ):

        assert factory_address is not None, "factory_address not set"
        assert router_address is not None, "router_address not set"
        assert init_code_hash is not None, "init_code_hash not set"
        
        self.web3 = web3
        self.factory_address = factory_address
        self.router_address = router_address
        self.init_code_hash = init_code_hash
        self.fee = fee
        
    def initialise_mock_data(self):
        """Set up mock data."""
        self.exchange_universe, self.pairs_table = UniswapV2MockClient.read_onchain_data(
            self.web3,
            self.factory_address,
            self.router_address,
            self.init_code_hash,
            self.fee,
        )
        assert len(self.pairs_table) > 0, f"Could not read any pairs from on-chain data. Uniswap v2 factory: {self.factory_address}, router: {self.router_address}."

    @staticmethod
    def read_onchain_data(
                web3: Web3,
                factory_address: HexAddress | str,
                router_address: HexAddress | str,
                init_code_hash: HexStr | str | NoneType = None,
                fee: float = 0.0030,
    ) -> Tuple[ExchangeUniverse, Table]:
        """Reads Uniswap v2 data from EVM backend and creates tables for it.

        - Read data from a single Uniswap v2 compatible deployment

        - Read all PairCreated events and constructed Pandas DataFrame out of them

        :param fee:
            Uniswap v2 do not have fee information available on-chain, so we need to pass it.

            Default to 30 BPS.
        """

        chain_id = ChainId(web3.eth.chain_id)

        # Get contracts
        Factory = get_contract(web3, "sushi/UniswapV2Factory.json")

        start_block = 1
        end_block = web3.eth.block_number

        if isinstance(web3.provider, HTTPProvider):
            endpoint_uri = web3.provider.endpoint_uri
        else:
            endpoint_uri = str(web3.provider)

        # Assume logging is safe, because this mock client is only to be used with testing backends
        logger.info("Scanning PairCreated events, %d - %d, from %s, factory is %s",
                    start_block,
                    end_block,
                    endpoint_uri,
                    factory_address
                    )

        filter = Filter.create_filter(
            factory_address,
            [Factory.events.PairCreated],
        )

        # Read through all the events, all the chain, using a single threaded slow loop.
        # Only suitable for test EVM backends.
        pairs = []
        log: LogResult
        for log in read_events(
            web3,
            start_block,
            end_block,
            filter=filter,
            extract_timestamps=None,
        ):
            # Signature this
            #
            #  event PairCreated(address indexed token0, address indexed token1, address pair, uint);
            #
            # topic 0 = keccak(event signature)
            # topic 1 = token 0
            # topic 2 = token 1
            # argument 0 = pair
            # argument 1 = pair id
            #
            # log for EthereumTester backend is
            #
            # {'type': 'mined',
            #  'logIndex': 0,
            #  'transactionIndex': 0,
            #  'transactionHash': HexBytes('0x2cf4563f8c275e5b5d7a4e5496bfbaf15cc00d530f15f730ac4a0decbc01d963'),
            #  'blockHash': HexBytes('0x7c0c6363bc8f4eac452a37e45248a720ff09f330117cdfac67640d31d140dc38'),
            #  'blockNumber': 6,
            #  'address': '0xF2E246BB76DF876Cef8b38ae84130F4F55De395b',
            #  'data': HexBytes('0x00000000000000000000000068931307edcb44c3389c507dab8d5d64d242e58f0000000000000000000000000000000000000000000000000000000000000001'),
            #  'topics': [HexBytes('0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'),
            #   HexBytes('0x0000000000000000000000002946259e0334f33a064106302415ad3391bed384'),
            #   HexBytes('0x000000000000000000000000b9816fc57977d5a786e654c7cf76767be63b966e')],
            #  'context': None,
            #  'event': web3._utils.datatypes.PairCreated,
            #  'chunk_id': 1,
            #  'timestamp': None}
            #
            arguments = decode_data(log["data"])
            topics = log["topics"]
            token0 = convert_uint256_hex_string_to_address(topics[1])
            token1 = convert_uint256_hex_string_to_address(topics[2])
            pair_address = convert_uint256_bytes_to_address(arguments[0])
            pair_id = convert_int256_bytes_to_int(arguments[1])

            token0_details = fetch_erc20_details(web3, token0)
            token1_details = fetch_erc20_details(web3, token1)

            # Our very primitive check to determine base token and quote token.
            # It's ok because this is a test backend
            if is_stablecoin_like(token0_details.symbol):
                quote_token_details = token0_details
                base_token_details = token1_details
            elif is_stablecoin_like(token1_details.symbol):
                quote_token_details = token1_details
                base_token_details = token0_details
            else:
                raise NotImplementedError(f"Does not know how to handle base-quote pairing for {token0_details} - {token1_details}. You need to update UniswapV2MockClient with more logic.")

            pair = DEXPair(
                    pair_id=pair_id,
                    chain_id=chain_id,
                    exchange_id=1,
                    exchange_slug="UniswapV2MockClient",
                    exchange_address=factory_address.lower(),
                    pair_slug=f"{base_token_details.symbol.lower()}-{quote_token_details.symbol.lower()}",
                    address=pair_address.lower(),
                    dex_type=ExchangeType.uniswap_v2,
                    base_token_symbol=base_token_details.symbol,
                    quote_token_symbol=quote_token_details.symbol,
                    token0_decimals=token0_details.decimals,
                    token1_decimals=token1_details.decimals,
                    token0_symbol=token0_details.symbol,
                    token1_symbol=token1_details.symbol,
                    token0_address=token0.lower(),
                    token1_address=token1.lower(),
                    buy_tax=0,
                    sell_tax=0,
                    transfer_tax=0,
                    fee=int(fee * 10_000),  #  Convert to BPS
                )
            pairs.append(pair)

        exchange = Exchange(
            chain_id=chain_id,
            chain_slug=chain_id.get_slug(),
            exchange_slug="UniswapV2MockClient",
            exchange_id=1,
            address=factory_address.lower(),
            exchange_type=ExchangeType.uniswap_v2,
            pair_count=len(pairs),
            default_router_address=router_address.lower(),
            init_code_hash=init_code_hash,
        )

        exchange_universe = ExchangeUniverse(exchanges={1: exchange})

        pair_table = DEXPair.convert_to_pyarrow_table(pairs, check_schema=False)

        return exchange_universe, pair_table





