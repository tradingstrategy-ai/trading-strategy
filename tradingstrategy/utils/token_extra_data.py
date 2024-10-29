"""High level helpers to deal with token tax data."""
import pandas as pd

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.top import TopPairsReply, TopPairMethod


def load_extra_metadata(
    pairs_df: pd.DataFrame,
    client: Client | None = None,
    top_pair_reply: TopPairsReply | None = None,
) -> pd.DataFrame:
    """Load token tax data for given pairs dataframe.

    - Supplements loaded trading pair data with additional metadata from /top API endpoint

    - Mainly used to get the token tax for trading pairs

    - Data is added by the base token, because TokenSniffer does not provide per-pair data

    .. note ::

        In the future, this data will be with supplied the core data,
        but due to heterogenous systems, you need to retrofit the data for pairs you need.

    .. warning::

        This is heavily under development.

    .. warning::

        Because we use third party services like TokenSniffer for token tax data,
        and often these services

    .. warning::

        The /top endpoint does not return data for dead trading pairs or assets. The trading pair
        must have seen at least $1 volume during the last 24h to be alive.

    :return:
        DataFrame with new columns added:

        - `buy_tax`
        - `sell_tax`
        - `top_pair_data` contains `TopPairData` instance for the base asset

    """

    assert isinstance(pairs_df, pd.DataFrame)

    assert len(pairs_df) > 0, "pairs_df is empty"
    assert len(pairs_df) < 200, f"pairs_df size is {len(pairs_df)}, looks too much?"

    if client is None:
        assert top_pair_reply is None, "Cannot give both client and top_pair_reply argument"

    assert "base_token_address" in pairs_df.columns, "base/quote token data must be retrofitted to the DataFrame before calling load_tokensniffer_metadata()"

    chain_id = ChainId(pairs_df.iloc[0]["chain_id"])
    token_addresses = pairs_df["base_token_address"].unique()

    # Load data if not given
    if top_pair_reply is None:
        top_pair_reply = client.fetch_top_pairs(
            {chain_id},
            addresses=token_addresses,
            method=TopPairMethod.by_token_addresses,
            min_volume_24h_usd=0,
            risk_score_threshold=0,
        )

    token_map = top_pair_reply.as_token_address_map()
    pairs_df["other_data"] = pairs_df["base_token_address"].apply(lambda x: {"top_pair_data": token_map[x]})
    pairs_df["buy_tax"] = pairs_df["other_data"].apply(lambda r: r["top_pair_data"].get_buy_tax())
    pairs_df["sell_tax"] = pairs_df["other_data"].apply(lambda r: r["top_pair_data"].get_sell_tax())
    return pairs_df
