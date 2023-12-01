"""Placeholder for Binance data."""

from tradingstrategy.pair import DEXPair
from tradingstrategy.binance.constants import (
    BINANCE_CHAIN_ID,
    BINANCE_EXCHANGE_ADDRESS,
    BINANCE_EXCHANGE_ID,
    BINANCE_FEE,
    BINANCE_SUPPORTED_QUOTE_TOKENS,
    BINANCE_EXCHANGE_SLUG,
    BINANCE_CHAIN_SLUG,
    BINANCE_EXCHANGE_TYPE,
    split_binance_symbol,
)
from tradingstrategy.utils.format import string_to_eth_address
from tradingstrategy.exchange import Exchange, ExchangeUniverse
from tradingstrategy.chain import ChainId


def generate_pairs_for_binance(
    symbols: list[str],
) -> list[DEXPair]:
    """Generate trading pair identifiers for Binance data.

    :param symbols: List of symbols to generate pairs for
    :return: List of trading pair identifiers
    """
    return [generate_pair_for_binance(symbol) for symbol in symbols]


def generate_pair_for_binance(
    symbol: str,
    fee: float = BINANCE_FEE,
    base_token_decimals: int = 18,
    quote_token_decimals: int = 18,
) -> DEXPair:
    """Generate a trading pair identifier for Binance data.

    Binance data is not on-chain, so we need to generate the identifiers
    for the trading pairs.

    .. note:: Internal exchange id is hardcoded to 129875571 and internal id to 134093847


    :param symbol: E.g. `ETHUSDT`
    :return: Trading pair identifier
    """
    assert 0 < fee < 1, f"Bad fee {fee}. Must be 0..1"

    assert symbol.endswith(BINANCE_SUPPORTED_QUOTE_TOKENS), f"Bad symbol {symbol}"

    base_token_symbol, quote_token_symbol = split_binance_symbol(symbol)
    base_token_address = string_to_eth_address(base_token_symbol)
    quote_token_address = string_to_eth_address(quote_token_symbol)

    pair_slug = f"{base_token_symbol}{quote_token_symbol}"

    return DEXPair(
        pair_id=symbol,
        chain_id=BINANCE_CHAIN_ID,
        exchange_id=BINANCE_EXCHANGE_ID,
        address=string_to_eth_address(pair_slug),
        base_token_symbol=base_token_symbol,
        quote_token_symbol=quote_token_symbol,
        token0_address=base_token_address,
        token0_symbol=base_token_symbol,
        token0_decimals=base_token_decimals,
        token1_address=quote_token_address,
        token1_symbol=quote_token_symbol,
        token1_decimals=quote_token_decimals,
        exchange_slug=BINANCE_EXCHANGE_SLUG,
        exchange_address=BINANCE_EXCHANGE_ADDRESS,
        fee=BINANCE_FEE * 10_000,
    )


def generate_exchange_for_binance(pair_count: int) -> Exchange:
    """Generate an exchange identifier for Binance data."""
    return Exchange(
        chain_id=BINANCE_CHAIN_ID,
        chain_slug=ChainId(BINANCE_CHAIN_SLUG),
        exchange_id=BINANCE_EXCHANGE_ID,
        exchange_slug=BINANCE_EXCHANGE_SLUG,
        address=BINANCE_EXCHANGE_ADDRESS,
        exchange_type=BINANCE_EXCHANGE_TYPE,
        pair_count=pair_count,
    )


def generate_exchange_universe_for_binance(pair_count: int) -> ExchangeUniverse:
    """Generate an exchange universe for Binance data."""
    return ExchangeUniverse.from_collection([generate_exchange_for_binance(pair_count)])
