from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import ExchangeType

BINANCE_CHAIN_ID = ChainId.unknown
BINANCE_CHAIN_SLUG = ChainId(BINANCE_CHAIN_ID)
BINANCE_EXCHANGE_ADDRESS = hex(hash("binance"))
BINANCE_EXCHANGE_SLUG = "binance"
BINANCE_EXCHANGE_ID = 129875571
BINANCE_EXCHANGE_TYPE = ExchangeType.uniswap_v2
BINANCE_FEE = 0.0005 # TODO: get correct fee
