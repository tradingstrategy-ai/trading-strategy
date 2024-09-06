"""Aggregated candles.

- Create aggregated price OHLCV and liquidity candles across all available DEX trading pairs

"""
import pandas as pd

def aggregate_ohlcv(
    pairs_df: pd.DataFrame,
    price_df: pd.DataFrame,
    liquidity_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Builds an aggregates dataframe for trading data.

    - Merge all pools e.g. ETH/USDC Uniswap v2, ETH/USDC Uniswap v3 30 BPS and 5 BPS to a single volume data

    - Prices are weighted by volume

    :param pairs_df:
        Pair metadata

    :param price_df:
        OHLCV dataframe.

        Must be forward filled.

    :param liquidity_df:
        Liquidity dataframe.

        Must be forward filled.

        Only "close" column is used.

    :return:
        DataFrame with following colmuns

        - base_token_symbol
        - open
        - low
        - high
        - close
        - volume
        - liquidity
        - pair_ids (list of ints)

        Volume and liquidity are in USD.
    """
    raise NotImplementedError()
