"""High level helpers to load with token tax data, TokenSniffer metadata and else."""
import logging

import pandas as pd

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.top import TopPairsReply, TopPairMethod
from tradingstrategy.utils.token_filter import POPULAR_QUOTE_TOKENS

logger = logging.getLogger(__name__)


def load_extra_metadata(
    pairs_df: pd.DataFrame,
    client: Client | None = None,
    top_pair_reply: TopPairsReply | None = None,
    ignored_tokens=POPULAR_QUOTE_TOKENS,
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

    Example:

    .. code-block:: python

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

    :return:
        DataFrame with new columns added:

        - `buy_tax`
        - `sell_tax`
        - `other_data` dict, contains `top_pair_data` which is `TopPairData` instance for the base asset

    """

    assert isinstance(pairs_df, pd.DataFrame)

    logger.info("Loading extra metadata for %d tokens", len(pairs_df))

    assert len(pairs_df) > 0, "pairs_df is empty"
    assert len(pairs_df) < 200, f"pairs_df size is {len(pairs_df)}, looks too much?"

    if client is None:
        assert top_pair_reply is None, "Cannot give both client and top_pair_reply argument"

    assert "base_token_address" in pairs_df.columns, "base/quote token address data must be retrofitted to the DataFrame before calling load_tokensniffer_metadata()"
    assert "base_token_symbol" in pairs_df.columns, "base/quote token symbol data must be retrofitted to the DataFrame before calling load_tokensniffer_metadata()"

    # Filter out quote tokens
    query_pairs_df = pairs_df.loc[~pairs_df["base_token_symbol"].isin(ignored_tokens)]

    chain_id = ChainId(pairs_df.iloc[0]["chain_id"])
    token_addresses = query_pairs_df["base_token_address"].unique()

    # Load data if not given
    if top_pair_reply is None:
        top_pair_reply = client.fetch_top_pairs(
            {chain_id},
            addresses=token_addresses,
            method=TopPairMethod.by_token_addresses,
            min_volume_24h_usd=0,
            risk_score_threshold=0,
        )

    # We retrofit data for the full frame,
    # ignored tokens just don't get these fields filled as None
    token_map = top_pair_reply.as_token_address_map()
    pairs_df["other_data"] = pairs_df["base_token_address"].apply(lambda x: {"top_pair_data": token_map.get(x)})
    pairs_df["buy_tax"] = pairs_df["other_data"].apply(lambda r: r["top_pair_data"] and r["top_pair_data"].get_buy_tax())
    pairs_df["sell_tax"] = pairs_df["other_data"].apply(lambda r: r["top_pair_data"] and r["top_pair_data"].get_sell_tax())
    return pairs_df
