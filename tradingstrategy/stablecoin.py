"""Stablecoin supported functions."""

#: Token symbols that are stablecoin like.
#: Note that it is *not* safe to to check the token symbol to know if a token is a specific stablecoin,
#: but you always need to check the contract address.
#: Checking against this list only works
STABLECOIN_LIKE = ("DAI", "USDC", "USDT", "DAI", "BUSD", "UST", "USDN", "LUSD", "VUSD", "USDV", "EUROe", "EURT", "USDP", "iUSD", "USDS", "gmUSD", "USDR", "RAI", "EURS", "TUSD", "EURe", "USD+", "EUROC", "USDs", "USDT.e", "USDC.e", "GHST", "jEUR", "crvUSD", "DOLA", "GUSD")

#: Stablecoins plus their interest wrapped counterparts on Compound and Aave.
#: Also contains other derivates.
WRAPPED_STABLECOIN_LIKE = ("cUSDC", "cUSDT", "sUSD", "aDAI", "cDAI", "tfUSDC", "alUSD", "agEUR", "gmdUSDC", "gDAI", "blUSD")

#: All stablecoin likes - both interested bearing and non interest bearing.
ALL_STABLECOIN_LIKE = STABLECOIN_LIKE + WRAPPED_STABLECOIN_LIKE

#: Tokens that are somehow wrapped/liquid staking/etc. and derive value from some other underlying token
#:
DERIVATIVE_TOKEN_PREFIXES = ["wst", "os", "3Crv", "gOHM", "st", "bl"]


def is_stablecoin_like(token_symbol: str, symbol_list=ALL_STABLECOIN_LIKE) -> bool:
    """Check if specific token symbol is likely a stablecoin.

    Useful for quickly filtering stable/stable pairs in the pools.
    However, you should never rely on this check alone.

    Note that new stablecoins might be introduced, so this check
    is never going to be future proof.

    :param token_symbol:
        Token symbol as it is written on the contract.
        May contain lower and uppercase latter.

    :param symbol_list:
        Which filtering list we use.
    """
    assert isinstance(token_symbol, str), f"We got {token_symbol}"
    return (token_symbol in symbol_list)


def is_derivative(token_symbol: str) -> bool:
    """Identify common derivate tokens.

    - They will have the same base value e.g. wstETH and ETH

    :return:
        True if token symbol matches a common known derivative token symbol pattern
    """
    assert isinstance(token_symbol, str), f"We got {token_symbol}"
    return any(token_symbol.startswith(prefix) for prefix in DERIVATIVE_TOKEN_PREFIXES)