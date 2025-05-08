"""Stablecoin filtering.

- See also :py:mod:`tradingstrategy.utils.token_filter` for more token types and filtering functionality.


"""

# Maintenance of stablecoin list moved to eth_defi package.
# Allow alias import here.
try:
    from eth_defi.token import ALL_STABLECOIN_LIKE
except ImportError:
    ALL_STABLECOIN_LIKE = set()


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

    assert ALL_STABLECOIN_LIKE, "eth_defi package must be installed to get the list of stablecoins"

    assert isinstance(token_symbol, str), f"We got {token_symbol}"
    return (token_symbol in symbol_list)

