"""Stablecoin filtering.

- See also :py:mod:`tradingstrategy.utils.token_filter` for more token types and filtering functionality.

"""

#: Token symbols that are stablecoin like.
#: Note that it is *not* safe to to check the token symbol to know if a token is a specific stablecoin,
#: but you always need to check the contract address.
#: Checking against this list only works
STABLECOIN_LIKE = {'ALUSD', 'BAC', 'BDO', 'BEAN', 'BOB', 'BUSD', 'CADC', 'CEUR', 'CJPY', 'CNHT', 'CRVUSD', 'CUSD', 'DAI', 'DJED', 'DOLA', 'DUSD', 'EOSDT', 'EURA', 'EUROC', 'EUROe', 'EURS', 'EURT', 'EURe', 'EUSD', 'FDUSD', 'FEI', 'FLEXUSD', 'FRAX', 'FXD', 'FXUSD', 'GBPT', 'GHO', 'GHST', 'GUSD', 'GYD', 'GYEN', 'HUSD', 'IRON', 'JCHF', 'JPYC', 'KDAI', 'LISUSD', 'LUSD', 'MIM', 'MIMATIC', 'MKUSD', 'MUSD', 'ONC', 'OUSD', 'PAR', 'PAXG', 'PYUSD', 'RAI', 'RUSD', 'SEUR', 'SFRAX', 'SILK', 'STUSD', 'SUSD', 'TCNH', 'TOR', 'TRYB', 'TUSD', 'USC', 'USD+', 'USDB', 'USDC', 'USDC.e', 'USDD', 'USDE', 'USDN', 'USDP', 'USDR', 'USDS', 'USDT', 'USDT.e', 'USDV', 'USDX', 'USDs', 'USK', 'UST', 'USTC', 'USX', 'UUSD', 'VAI', 'VEUR', 'VST', 'VUSD', 'XAUT', 'XDAI', 'XIDR', 'XSGD', 'XSTUSD', 'XUSD', 'YUSD', 'ZSD', 'ZUSD', 'gmUSD', 'iUSD', 'jEUR', 'crvUSD', 'USDe', 'kUSD', 'sosUSDT'}

#: Stablecoins plus their interest wrapped counterparts on Compound and Aave.
#: Also contains other derivates.
WRAPPED_STABLECOIN_LIKE = {"cUSDC", "cUSDT", "sUSD", "aDAI", "cDAI", "tfUSDC", "alUSD", "agEUR", "gmdUSDC", "gDAI", "blUSD"}

#: All stablecoin likes - both interested bearing and non interest bearing.
ALL_STABLECOIN_LIKE = STABLECOIN_LIKE | WRAPPED_STABLECOIN_LIKE


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

