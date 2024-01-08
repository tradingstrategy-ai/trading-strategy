"""Utility functions to help automatically generate Binance data."""
import pandas as pd
from eth_typing import HexAddress


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
from tradingstrategy.utils.format import string_to_eth_address
from tradingstrategy.lending import (
    LendingReserve,
    LendingProtocolType,
    LendingReserveAdditionalDetails,
)
from tradingstrategy.pair import DEXPair


def generate_pairs_for_binance(
    symbols: list[str],
    fee: float = BINANCE_FEE,
) -> list[DEXPair]:
    """Generate trading pair identifiers for Binance data.

    :param symbols: List of symbols to generate pairs for
    :param fee: fee override for the trading pairs in float form, to make trading cost the same as DEX
    :return: List of trading pair identifiers
    """
    return [generate_pair_for_binance(symbol, fee) for symbol in symbols]


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
    :param fee: fee for the pair in float form
    :param base_token_decimals: decimals for the base token
    :param quote_token_decimals: decimals for the quote token
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
        fee=fee * 10_000,
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


def add_info_columns_to_ohlc(df: pd.DataFrame, pairs: dict[str, DEXPair]):
    """Add single pair informational columns to an OHLC dataframe.

    :param *args: Each argument is a dict with the format {symbol: pair}
        E.g. {'ETHUSDT': TradingPairIdentifier(...)}

    :return: The same dataframe with added columns
    """

    for symbol, pair in pairs.items():
        if symbol not in df["pair_id"].unique():
            raise ValueError(f"Symbol {symbol} not found in DataFrame. Pair ids are {list(df['pair_id'].unique())}")

        # Update the DataFrame only for the rows where 'symbol' matches
        mask = df["pair_id"] == symbol
        df.loc[mask, "base_token_symbol"] = pair.base_token_symbol
        df.loc[mask, "quote_token_symbol"] = pair.quote_token_symbol
        df.loc[mask, "exchange_slug"] = pair.exchange_slug
        df.loc[mask, "chain_id"] = pair.chain_id
        df.loc[mask, "fee"] = pair.fee / 10_000
        df.loc[mask, "buy_volume_all_time"] = 0
        df.loc[mask, "address"] = pair.address
        df.loc[mask, "exchange_id"] = BINANCE_EXCHANGE_ID
        df.loc[mask, "token0_address"] = pair.base_token_address
        df.loc[mask, "token1_address"] = pair.quote_token_address
        df.loc[mask, "token0_symbol"] = pair.base_token_symbol
        df.loc[mask, "token1_symbol"] = pair.quote_token_symbol
        df.loc[mask, "token0_decimals"] = pair.base_token_decimals
        df.loc[mask, "token1_decimals"] = pair.quote_token_decimals

    return df


def generate_lending_reserve_for_binance(
    asset_symbol: str,
    address: HexAddress,
    reserve_id: int,
    asset_decimals=18,
) -> LendingReserve:
    """Generate a lending reserve for Binance data.

    Binance data is not on-chain, so we need to generate the identifiers
    for the trading pairs.

    :param asset_symbol: E.g. `ETH`
    :param address: address of the reserve
    :param reserve_id: id of the reserve
    :return: LendingReserve
    """

    assert isinstance(reserve_id, int), f"Bad reserve_id {reserve_id}"

    atoken_symbol = f"a{asset_symbol.upper()}"
    vtoken_symbol = f"v{asset_symbol.upper()}"

    return LendingReserve(
        reserve_id=reserve_id,
        reserve_slug=asset_symbol.lower(),
        protocol_slug=LendingProtocolType.aave_v3,
        chain_id=BINANCE_CHAIN_ID,
        chain_slug=BINANCE_CHAIN_SLUG,
        asset_id=1,
        asset_symbol=asset_symbol,
        asset_address=address,
        asset_decimals=asset_decimals,
        atoken_id=1,
        asset_name=asset_symbol,
        atoken_symbol=atoken_symbol,
        atoken_address=string_to_eth_address(atoken_symbol),
        atoken_decimals=18,
        vtoken_id=1,
        vtoken_symbol=vtoken_symbol,
        vtoken_address=string_to_eth_address(vtoken_symbol),
        vtoken_decimals=18,
        additional_details=LendingReserveAdditionalDetails(
            ltv=0.825,
            liquidation_threshold=0.85,
        ),
    )
