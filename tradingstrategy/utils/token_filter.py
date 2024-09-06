"""Filter out tokens and trading pairs.

- Works mainly on trading pairs dataframe - see :py:mod:`tradingstrategy.pairs`

- See also: :py:mod:`tradingstrategy.stablecoin` for different stablecoin whitelists

- For easy filtering "give me tradeable trading pairs universe from these pairs" see :py:func:`filter_default`

"""

def filter_for_base_tokens(
    pairs: pd.DataFrame,
    base_token_addresses: List[str] | Set[str]
) -> pd.DataFrame:
    """Filter dataset so that it only contains data for the trading pairs that have a certain base token.

    Useful as a preprocess step for creating :py:class:`tradingstrategy.lending.LendingUniverse`

    Example:

    .. code-block:: python

        client = persistent_test_client

        exchange_universe = client.fetch_exchange_universe()

        quote_tokens = {
            "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",  # USDC polygon
            "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",  # USDT polygon
        }

        pairs_df = client.fetch_pair_universe().to_pandas()

        # Find out all volatile pairs traded against USDC and USDT on Polygon
        pairs_df = filter_for_chain(pairs_df, ChainId.polygon)
        pairs_df = filter_for_stablecoins(pairs_df, StablecoinFilteringMode.only_volatile_pairs)
        pairs_df = filter_for_quote_tokens(pairs_df, quote_tokens)

        # Create lending universe and trading universe with the cross section of
        # - Available assets in the lending protocols
        # - Asset we can trade
        lending_reserves = client.fetch_lending_reserve_universe()
        pairs_df = filter_for_base_tokens(pairs_df, lending_reserves.get_asset_addresses())

        pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

        # Lending reserves have around ~320 individual trading pairs on Polygon across different DEXes
        assert 1 < pair_universe.get_count() < 1_000

        eth_usdc = pair_universe.get_pair_by_human_description((ChainId.polygon, "uniswap-v3", "WETH", "USDC"))

    :param quote_token_addresses:
        List of Ethereum addresses of the tokens.

        Lowercased, non-checksummed.

    :return:
        DataFrame with trading pairs filtered to match quote token condition
    """
    assert type(base_token_addresses) in (list, set), f"Received: {type(base_token_addresses)}: {base_token_addresses}"

    for addr in base_token_addresses:
        assert addr == addr.lower(), f"Address was not lowercased {addr}"

    our_pairs: pd.DataFrame = pairs.loc[
        (pairs['token0_address'].isin(base_token_addresses) & (pairs['token0_symbol'] == pairs['base_token_symbol'])) |
        (pairs['token1_address'].isin(base_token_addresses) & (pairs['token1_symbol'] == pairs['base_token_symbol']))
    ]

    return our_pairs


def filter_for_quote_tokens(
        pairs: pd.DataFrame,
        quote_token_addresses: List[str] | Set[str]
) -> pd.DataFrame:
    """Filter dataset so that it only contains data for the trading pairs that have a certain quote tokens.

    Useful as a preprocess step for creating :py:class:`tradingstrategy.candle.GroupedCandleUniverse`
    or :py:class:`tradingstrategy.liquidity.GroupedLiquidityUniverse`.

    You might, for example, want to construct a trading universe where you have only BUSD pairs.

    Example:

    .. code-block:: python

        exchange_universe = client.fetch_exchange_universe()

        quote_tokens = {
            "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",  # USDC polygon
            "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",  # USDT polygon
        }

        pairs_df = client.fetch_pair_universe().to_pandas()

        # Find out all volatile pairs traded against USDC and USDT on Polygon
        pairs_df = filter_for_chain(pairs_df, ChainId.polygon)
        pairs_df = filter_for_stablecoins(pairs_df, StablecoinFilteringMode.only_volatile_pairs)
        pairs_df = filter_for_quote_tokens(pairs_df, quote_tokens)

        pairs_df = filter_for_quote_tokens(pairs_df, lending_reserves.get_asset_addresses())
        pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

    :param quote_token_addresses:
        List of Ethereum addresses of the tokens.

        Lowercased, non-checksummed.

    :return:
        DataFrame with trading pairs filtered to match quote token condition
    """
    assert type(quote_token_addresses) in (list, set), f"Received: {type(quote_token_addresses)}: {quote_token_addresses}"

    for addr in quote_token_addresses:
        assert addr == addr.lower(), f"Address was not lowercased {addr}"

    our_pairs: pd.DataFrame = pairs.loc[
        (pairs['token0_address'].isin(quote_token_addresses) & (pairs['token0_symbol'] == pairs['quote_token_symbol'])) |
        (pairs['token1_address'].isin(quote_token_addresses) & (pairs['token1_symbol'] == pairs['quote_token_symbol']))
    ]

    return our_pairs


def filter_for_blacklisted_tokens(
    pairs: pd.DataFrame,
    blacklisted_tokens: List[str] | Set[str]
) -> pd.DataFrame:
    """Remove blacklisted tokens from the trading pair set.

    Useful as a preprocess step for creating trading pairs that cause issues in open-ended strategies.
    Example:

    .. code-block:: python

        avoid_backtesting_tokens = {

            # MKR market is created and pulled out,
            # leaving us no good price source in the dataset
            # https://tradingstrategy.ai/trading-view/ethereum/uniswap-v3/mkr-usdc-fee-5#7d
            "MKR",

            # Not sure what's going on with this token,
            # price action and TVL not normal though 100k liquidity
            # https://tradingstrategy.ai/trading-view/ethereum/uniswap-v3/sbio-usdc-fee-30#1d
            "SBIO",

            # Same problems as MKR,
            # it has historical TVL that then gets pulled down to zero
            # https://tradingstrategy.ai/trading-view/ethereum/uniswap-v3/ldo-usdc-fee-30
            "LDO",

            # Trading jsut stops (though there is liq left)
            # https://tradingstrategy.ai/trading-view/ethereum/uniswap-v3/id-usdc-fee-30
            "ID",

            # Disappearing market, as above
            "DMT",
            "XCHF",
            "FLC",
            "GF",
            "CVX",
            "MERC",
            "ICHI",
            "DOVU",
            "DOVU[eth]",
            "DHT",
            "EWIT",

            # Abnormal price during the rebalance
            # adjust_position() does not have good price checks /
            # how to recover in the case price goes hayware after opening the position
            "MAP",
            "TRX",
            "LAI",
        }
        tradeable_pairs_df = client.fetch_pair_universe().to_pandas()
        tradeable_pairs_df = filter_for_blacklisted_tokens(tradeable_pairs_df, avoid_backtesting_tokens)
        print("Pairs without blacklisted base token", len(tradeable_pairs_df))

    :param blacklisted_tokens:
        Blacklisted token symbols or addresses.

    :return:
        DataFrame with trading pairs filtered to match quote token condition
    """
    assert type(blacklisted_tokens) in (list, set), f"Received: {type(blacklisted_tokens)}: {blacklisted_tokens}"

    blacklisted_tokens = [t.lower() for t in blacklisted_tokens]

    blacklisted_mask = \
        pairs['token0_address'].isin(blacklisted_tokens) | \
        pairs['token0_symbol'].str.lower().isin(blacklisted_tokens) | \
        pairs['token1_address'].isin(blacklisted_tokens) | \
        pairs['token1_symbol'].str.lower().isin(blacklisted_tokens)

    return pairs[~blacklisted_mask]


def filter_for_nonascii_tokens(
    pairs: pd.DataFrame,
) -> pd.DataFrame:
    """Remove tokens with unprintable characters

    - Emojis

    - Some crap tokens like 20SML025��������

    - There should be no legit tokens with non-ASCII names

    :return:
        DataFrame with trading pairs filtered to match quote token condition
    """
    def has_non_ascii(text):
        return any(ord(char) > 127 for char in text)

    def my_filter(row):
        return has_non_ascii(row.token0_symbol) or has_non_ascii(row.token1_symbol)

    blacklisted_mask = pairs.apply(my_filter, axis=1)

    return pairs[~blacklisted_mask]


class StablecoinFilteringMode(enum.Enum):
    """How to filter pairs in stablecoin filtering.

    See :py:func:`filter_for_stablecoins`.
    """

    #: Stable-stable pairs
    only_stablecoin_pairs = "only_stablecoin_pairs"

    #: Volatile pairs
    #:
    #: Usually this is "tradeable" pairs
    #:
    only_volatile_pairs = "only_volatile_pairs"

    #: Any trading pair
    all_pairs = "all_pairs"


def filter_for_stablecoins(pairs: pd.DataFrame, mode: StablecoinFilteringMode) -> pd.DataFrame:
    """Filter dataset so that it only contains data for the trading pairs that are either stablecoin pairs or not.

    Trading logic might not be able to deal with or does not want to deal with stable -> stable pairs.
    Trading stablecoin to another does not make sense, unless you are doing high volume arbitration strategies.

    Uses internal stablecoin list from :py:mod:`tradingstrategy.stablecoin`.

    - For code example see :py:func:`filter_for_quote_tokens`
    - See also :py:class:`StablecoinFilteringMode`

    Example:

    .. code-block:: python

        from tradingstrategy.pair import filter_for_stablecoins, StablecoinFilteringMode

        # Remove pairs with expensive 1% fee tier
        # Remove stable-stable pairs
        tradeable_pairs_df = pairs_df.loc[pairs_df["fee"] <= 30]
        tradeable_pairs_df = filter_for_stablecoins(tradeable_pairs_df, StablecoinFilteringMode.only_volatile_pairs)

        # Narrow down candle data to pairs that are left after filtering
        candles_df = candles_df.loc[candles_df["pair_id"].isin(tradeable_pairs_df["pair_id"])]

        print(f"We have {len(tradeable_pairs_df)} tradeable pairs")

    :param pairs:
        DataFrame of of :py:class:`tradingstrategy.pair.DEXPair`

    :param mode:
         Are we looking for stablecoin pairs or volatile pairs

    :return:
        Filtered DataFrame
    """
    assert isinstance(mode, StablecoinFilteringMode)

    if mode == StablecoinFilteringMode.all_pairs:
        return pairs

    if mode == StablecoinFilteringMode.only_stablecoin_pairs:
        our_pairs: pd.DataFrame = pairs.loc[
            (pairs['token0_symbol'].isin(ALL_STABLECOIN_LIKE) & pairs['token1_symbol'].isin(ALL_STABLECOIN_LIKE))
        ]
    else:
        # https://stackoverflow.com/a/35939586/315168
        our_pairs: pd.DataFrame = pairs.loc[
            ~(pairs['token0_symbol'].isin(ALL_STABLECOIN_LIKE) & pairs['token1_symbol'].isin(ALL_STABLECOIN_LIKE))
        ]
    return our_pairs


def filter_for_derivatives(pairs: pd.DataFrame, derivatives=False) -> pd.DataFrame:
    """Detect derivative token.

    - These tokens do not present underlying trading pair, but derive their value
      from some other token e.g. `stETH` in `stETH/ETH`

    - They behave as stable/stable pairs

    :param derivatives:
        Set false to exclude derivative token, True to have only them.
    """

    assert isinstance(pairs, pd.DataFrame)

    def row_filter(row):
        if derivatives:
            return is_derivative(row["token0_symbol"]) or is_derivative(row["token1_symbol"])
        else:
            return (not is_derivative(row["token0_symbol"])) and (not is_derivative(row["token1_symbol"]))

    df =  pairs[pairs.apply(row_filter, axis=1)]
    return df


def filter_for_chain(
    pairs: pd.DataFrame,
    chain_id: ChainId,
):
    """Extract trading pairs for specific blockchain.

    - For code example see :py:func:`filter_for_quote_tokens`
    """
    assert isinstance(chain_id, ChainId)
    return pairs.loc[pairs["chain_id"] == chain_id.value]


def filter_for_exchange(
    pairs: pd.DataFrame,
    exchange_slug: Slug | Set[Slug] | Tuple[Slug] | List[Slug],
):
    """Extract trading pairs for specific exchange(s).

    Example:

    .. code-block:: python

        # Pick only pairs traded on Uniswap v3
        df = filter_for_exchange(df, "uniswap-v3")

    With two exchanges:

        # Pick only pairs traded on Uniswap v3 or Quickswap
        df = filter_for_exchange(df, {"uniswap-v3", "quickswap"})

    """
    if type(exchange_slug) == str:
        return pairs.loc[pairs["exchange_slug"] == exchange_slug]
    elif type(exchange_slug) in (tuple, set, list):
        return pairs.loc[pairs["exchange_slug"].isin(exchange_slug)]
    else:
        raise AssertionError(f"Unsupported exchange slug filter: {exchange_slug.__class__}")


def filter_pairs_default(
    pairs: pd.DataFrame,
):
    """Filter out pairs that are not interested for trading.

    This includes

    - Non-volatile pairs (stETH/ETH) - :py:func:`filter_for_stablecoins`

    - Derivate pairs (stETH/ETH) - :py:func:`filter_for_derivatives`
    """
