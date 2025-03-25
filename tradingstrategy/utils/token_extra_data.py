"""High level helpers to load with token tax data, TokenSniffer metadata and else."""
import logging
from math import isnan

import numpy as np
import pandas as pd

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.token_metadata import TokenMetadata
from tradingstrategy.top import TopPairsReply, TopPairMethod
from tradingstrategy.utils.token_filter import POPULAR_QUOTE_TOKENS


logger = logging.getLogger(__name__)


def load_extra_metadata(
    pairs_df: pd.DataFrame,
    client: Client | None = None,
    top_pair_reply: TopPairsReply | None = None,
    ignored_tokens=POPULAR_QUOTE_TOKENS,
    min_volume_24h_usd=0,
    risk_score_threshold=0, 
) -> pd.DataFrame:
    """Load token tax data for given pairs dataframe.

    - Supplements loaded trading pair data with additional metadata from /top API endpoint

    - Mainly used to get the token tax for trading pairs

    - Data is added by the base token, because TokenSniffer does not provide per-pair data

    - Can only handle token amounts /top endpoint can handle

    .. note ::

        In the future, this data will be with supplied the core data,
        but due to heterogenous systems, you need to retrofit the data for pairs you need.

    .. warning::

        This is heavily under development.

    .. warning::

        Because we use third party services like TokenSniffer for token tax data,
        and often these services key this data by tokens, not by trading pairs,
        this data might be invalid per trading pair.

    .. warning::

        The /top endpoint may not return data for dead trading pairs or assets. The trading pair
        must have seen at least $1 volume during the last 24h to be alive, or other similar condition.

    Example how to perform scam filter on pair universe data:

    .. code-block:: python

        # Scam filter using TokenSniffer
        pairs_df = load_extra_metadata(
            pairs_df,
            client,
        )
        all_pairs_df = pairs_df
        pairs_df = pairs_df.loc[pairs_df["risk_score"] >= Parameters.min_token_sniffer_score]
        print(f"After scam filter we have {len(pairs_df)} pairs")
        clean_tokens = pairs_df["base_token_symbol"]
        only_scams = all_pairs_df.loc[~all_pairs_df["base_token_symbol"].isin(clean_tokens)]
        for idx, row in only_scams.iterrows():
            print(f"Scammy pair {row.base_token_symbol} - {row.quote_token_symbol}, risk score {row.risk_score}, pool {row.address}, token {row.base_token_address}")

        pairs_df = pairs_df.sort_values("volume", ascending=False)

        print("Top pair matches (including benchmark pairs):")
        for _, pair in pairs_df.head(10).iterrows():
            print(f"   Pair: {pair.base_token_symbol} - {pair.quote_token_symbol} ({pair.exchange_slug})")

    Another example:

    .. code-block:: python

        from tradingstrategy.utils.token_extra_data import load_extra_metadata

        exchange_universe = client.fetch_exchange_universe()

        addresses = [
            "0x71fc7cf3e26ce5933fa1952590ca6014a5938138",  # FRIEND.TECH 0x71fc7cf3e26ce5933fa1952590ca6014a5938138 SCAM
            "0x14feE680690900BA0ccCfC76AD70Fd1b95D10e16",  # $PAAL 0x14feE680690900BA0ccCfC76AD70Fd1b95D10e16
            "0x576e2BeD8F7b46D34016198911Cdf9886f78bea7"   # TRUMP 0x576e2BeD8F7b46D34016198911Cdf9886f78bea7
        ]
        addresses = list(map(str.lower, addresses))

        # Get all pairs data and filter to our subset
        pairs_df = client.fetch_pair_universe().to_pandas()
        pairs_df = add_base_quote_address_columns(pairs_df)
        pairs_df = pairs_df.loc[
            (pairs_df["base_token_address"].isin(addresses)) &
            (pairs_df["chain_id"] == 1)
        ]

        # Retrofit TokenSniffer data
        pairs_df = load_extra_metadata(
            pairs_df,
            client=client,
        )

        assert isinstance(pairs_df, pd.DataFrame)
        assert "buy_tax" in pairs_df.columns
        assert "sell_tax" in pairs_df.columns
        assert "other_data" in pairs_df.columns

        #
        pair_universe = PandasPairUniverse(
            pairs_df,
            exchange_universe=exchange_universe,
        )

        trump_weth = pair_universe.get_pair_by_human_description(
            (ChainId.ethereum, "uniswap-v2", "TRUMP", "WETH"),
        )

        # Read buy/sell/tokensniffer metadta through DEXPair instance
        assert trump_weth.buy_tax == pytest.approx(1.0, rel=0.02)
        assert trump_weth.sell_tax == pytest.approx(1.0, rel=0.02)
        assert trump_weth.token_sniffer_data.get("balances") is not None  # Read random column from TokenSniffer reply

    :param pairs_df:
        Pandas DataFrame with pairs data.

        Must be retrofitted with `add_base_quote_address_columns()`.

    :param client:
        Give client to load /top metadata

    :param top_pair_reply:
        Pass preloaded /top metadata

    :param ignored_tokens:
        Ignore popular quote tokens.

        Asking data for these tokens causes too many hits and pollutes the query.
        The column `risk_score` column is set to 100 for these tokens.

    :return:
        DataFrame with new columns added:

        - `buy_tax`
        - `sell_tax`
        - `other_data` dict, contains `top_pair_data` which is `TopPairData` instance for the base asset
        - `risk_score` - risk score 0 to 100, we recommend to cull everything beloe 65
        - `whitelisted` - token is on `ignored_tokens` whitelist

    """

    assert isinstance(pairs_df, pd.DataFrame)

    logger.info("Loading extra metadata for %d tokens", len(pairs_df))

    assert len(pairs_df) > 0, "pairs_df is empty"
    assert len(pairs_df) < 800, f"pairs_df size is {len(pairs_df)}, looks too much?"

    if client is None:
        assert top_pair_reply is None, "Cannot give both client and top_pair_reply argument"

    assert "base_token_address" in pairs_df.columns, "base/quote token address data must be retrofitted to the DataFrame before calling load_tokensniffer_metadata(). Call add_base_quote_address_columns() first."
    assert "base_token_symbol" in pairs_df.columns, "base/quote token symbol data must be retrofitted to the DataFrame before calling load_tokensniffer_metadata(). Call add_base_quote_address_columns() first."

    # Filter out quote tokens
    query_pairs_df = pairs_df.loc[~pairs_df["base_token_symbol"].isin(ignored_tokens)]

    logger.info(
        "Total queried tokens will be %d",
        len(query_pairs_df),
    )

    chain_id = ChainId(pairs_df.iloc[0]["chain_id"])
    token_addresses = query_pairs_df["base_token_address"].unique()

    logger.info("Querying %d unique base tokens", len(token_addresses))

    # Load data if not given
    if top_pair_reply is None:
        top_pair_reply = client.fetch_top_pairs(
            {chain_id},
            addresses=token_addresses,
            method=TopPairMethod.by_token_addresses,
            min_volume_24h_usd=min_volume_24h_usd,
            risk_score_threshold=risk_score_threshold,
            # limit=len(pairs_df) * 2,  # Assume max 2 pairs per token
        )

    # We retrofit data for the full frame,
    # ignored tokens just don't get these fields filled as None
    token_map = top_pair_reply.as_token_address_map()

    logger.info(
        "We got metadata for %d tokens",
        len(token_map),
    )

    pairs_df["other_data"] = pairs_df["base_token_address"].apply(lambda x: {"top_pair_data": token_map.get(x)})
    pairs_df["buy_tax"] = pairs_df["other_data"].apply(lambda r: r["top_pair_data"] and r["top_pair_data"].get_buy_tax())
    pairs_df["sell_tax"] = pairs_df["other_data"].apply(lambda r: r["top_pair_data"] and r["top_pair_data"].get_sell_tax())
    pairs_df["risk_score"] = pairs_df["other_data"].apply(lambda r: r["top_pair_data"] and r["top_pair_data"].token_sniffer_score)
    pairs_df["whitelisted"] = pairs_df["base_token_symbol"].apply(lambda r: r in ignored_tokens)
    pairs_df.loc[pairs_df["whitelisted"], 'risk_score'] = 100
    return pairs_df



def filter_scams(
    pairs_df: pd.DataFrame,
    client: Client,
    min_token_sniffer_score=65,
    drop_token_tax=False,
    min_volume_24h_usd=0,
    risk_score_threshold=0,    
    verbose=True,
) -> pd.DataFrame:
    """Filter out scam tokens in pairs dataset and print some stdout diagnostics.

    - Loads token extra metadata from the server for the trading pairs

    - This includes TokenSniffer scores

    - Zero means zero TokenSniffer score. Nan means the TokenSniffer data was not available on the server likely due to low liquidity/trading pair no longer functional.

    .. note::

        Deprecated. Use :py:func:`load_token_metadata` instead.

    Example:

    .. code-block:: python

        # Scam filter using TokenSniffer
        pairs_df = filter_scams(pairs_df, client, min_token_sniffer_score=Parameters.min_token_sniffer_score)
        pairs_df = pairs_df.sort_values("volume", ascending=False)

        print("Top pair matches (including benchmark pairs):")
        for _, pair in pairs_df.head(10).iterrows():
            print(f"   Pair: {pair.base_token_symbol} - {pair.quote_token_symbol} ({pair.exchange_slug})")

        uni_v2 = pairs_df.loc[pairs_df["exchange_slug"] == "uniswap-v2"]
        uni_v3 = pairs_df.loc[pairs_df["exchange_slug"] == "uniswap-v3"]
        print(f"Pairs on Uniswap v2: {len(uni_v2)}, Uniswap v3: {len(uni_v3)}")

        dataset = load_partial_data(
            client=client,
            time_bucket=Parameters.candle_time_bucket,
            pairs=pairs_df,
            execution_context=execution_context,
            universe_options=universe_options,
            liquidity=True,
            liquidity_time_bucket=TimeBucket.d1,
        )

    :parma drop_token_tax:
        Discard tokens with token tax features, as by Tokensniffer data

    :param min_token_sniffer_score:
        Do not load data from the server unless score is higher

    :param min_volume_24h_usd:
        Ignore pairs with less than this 24h volume.

        Set to zero to load historical scams.

    :param verbose:
        Print out data about scams

    """
    pairs_df = load_extra_metadata(
        pairs_df,
        client,
        min_volume_24h_usd=min_volume_24h_usd,
        risk_score_threshold=risk_score_threshold,
    )
    all_pairs_df = pairs_df
    pairs_df = pairs_df.loc[pairs_df["risk_score"] >= min_token_sniffer_score]
    if verbose:
        print(f"After scam filter we have {len(pairs_df)} pairs. Zero means zero TokenSniffer score. Nan means the TokenSniffer data was not available on the server likely due to low liquidity/trading pair no longer functional.")
    clean_tokens = pairs_df["base_token_symbol"]
    only_scams = all_pairs_df.loc[~all_pairs_df["base_token_symbol"].isin(clean_tokens)]

    if verbose:
        for _, row in only_scams.iterrows():
            print(f"Scammy pair {row.base_token_symbol} - {row.quote_token_symbol}, risk score {row.risk_score}, pool {row.address}, token {row.base_token_address}")

    if drop_token_tax:
        taxed_token_mask = (pairs_df["buy_tax"] > 0) | (pairs_df["sell_tax"] > 0)
        taxed_tokens = pairs_df[taxed_token_mask]
        pairs_df = pairs_df[~taxed_token_mask]

        if verbose:
            for _, row in taxed_tokens.iterrows():
                print(f"Taxed pair {row.base_token_symbol} - {row.quote_token_symbol}, buy tax {row.buy_tax * 100} %, sell tax {row.sell_tax * 100} %, pool {row.address}, token {row.base_token_address}")

    return pairs_df


def load_token_metadata(
    pairs_df: pd.DataFrame,
    client: Client,
    printer=lambda x: logger.info(x),
) -> pd.DataFrame:
    """Load token metadata for all trading pairs.

    - Load and cache token metadata for given DataFrame of trading pairs
    - Gets Trading Strategy metadata, TokenSniffer data, Coingecko data
    - Uses :py:meth:`Client.fetch_token_metadata` to retrofit trading pair data with token metadata
    - Can be used e.g. for scam filtering

    See :py:class:`tradingstrategy.token_metadata.TokenMetadata`.
    :return:
        New DataFrame with new columns:

        - "token_metadata" containing token metadata object.
        - "coingecko_categories" containing CoinGecko categories
        - "tokensniffer_score" containing TokenSniffer risk score
        - "tokensniffer_error" containing error message if TokenSniffer data could not be fetched for a token
        - "buy_tax" containing buy tax
        - "sell_tax" containing sell tax

        All data is for the base token of the trading pair.
        Columns will contain ``None`` value if not available.
    """

    assert isinstance(pairs_df, pd.DataFrame)

    assert "base_token_address" in pairs_df.columns, "base/quote token address data must be retrofitted to the DataFrame before calling load_tokensniffer_metadata(). Call add_base_quote_address_columns() first."
    assert "base_token_symbol" in pairs_df.columns, "base/quote token symbol data must be retrofitted to the DataFrame before calling load_tokensniffer_metadata(). Call add_base_quote_address_columns() first."

    token_addresses = set(pd.concat([pairs_df["token0_address"], pairs_df["token1_address"]]))

    chain_ids = pairs_df["chain_id"].unique()
    assert len(chain_ids) != 0, "No pairs to load in load_token_metadata()"
    assert len(chain_ids) == 1, f"Mixed chain_ids: {chain_ids}"
    chain_id = ChainId(chain_ids[0])

    printer(f"Loading metadata for {len(token_addresses)} base tokens in {len(pairs_df)} trading pairs")

    token_metadata = client.fetch_token_metadata(
        chain_id,
        token_addresses
    )

    printer(f"Got data back for {len(token_metadata)} tokens")

    def _map_meta(address):
        data = token_metadata.get(address)
        if data:
            return data
        return None

    def _map_sniffer_data(meta: TokenMetadata | None):
        if meta:
            return meta.token_sniffer_data
        return None

    def _map_risk_score(meta: TokenMetadata | None):
        if meta:
            return meta.token_sniffer_score
        return None

    def _map_categories(meta: TokenMetadata | None):
        if meta:
            return  meta.get_coingecko_categories()
        return None

    def _map_buy_tax(meta: TokenMetadata | None):
        if meta:
            return meta.get_buy_tax()
        return None

    def _map_sell_tax(meta: TokenMetadata | None):
        if meta:
            return meta.get_sell_tax()
        return None

    def _map_sniff_error(meta: TokenMetadata | None):
        if meta:
            return meta.token_sniffer_error
        return None

    df = pairs_df
    df["token_metadata"] = df["base_token_address"].apply(_map_meta)
    df["tokensniffer_metadata"] = df["token_metadata"].apply(_map_sniffer_data)
    df["tokensniffer_score"] = df["token_metadata"].apply(_map_risk_score)
    df["tokensniffer_error"] = df["token_metadata"].apply(_map_sniff_error)
    df["coingecko_categories"] = df["token_metadata"].apply(_map_categories)
    df["buy_tax"] = df["token_metadata"].apply(_map_buy_tax)
    df["sell_tax"] = df["token_metadata"].apply(_map_sell_tax)

    error_count = df["tokensniffer_error"].notna().sum()
    printer(f"TokenSniffer has {error_count} error entries")

    missing_meta_mask = df["token_metadata"].isna()
    missing_meta_df = df[missing_meta_mask]
    if len(missing_meta_df) > 0:
        display_df = missing_meta_df[["pair_id", "base_token_symbol", "base_token_address"]]
        assert not df["token_metadata"].isna().any().any(), f"NA detected in token metadata, {len(missing_meta_df)} entries missing\nDid metadata download crash?:\n{display_df}"

    return df
