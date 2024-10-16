"""Test /top endpoint."""
import datetime

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.top import TopPairsReply


def test_load_top(persistent_test_client: Client):
    """Load 10 top pairs by liquidity from /top endpoint.

    - Integration test

    - Get whatever pairs we have today
    """

    client = persistent_test_client

    top_reply = client.fetch_top_pairs(
        chain_ids={ChainId.ethereum},
        exchange_slugs={"uniswap-v2", "uniswap-v3"},
        limit=10,
    )

    assert isinstance(top_reply, TopPairsReply)
    assert len(top_reply.included) == 10
    assert len(top_reply.excluded) > 0  # There is always something to be excluded

    # Because this is a dynamic reply,
    # we just check accessor methods work
    for pair in top_reply.included:
        assert pair.get_persistent_id() is not None
        assert isinstance(pair.volume_updated_at, datetime.datetime)
        assert isinstance(pair.tvl_updated_at, datetime.datetime)
        assert isinstance(pair.queried_at, datetime.datetime)
        assert pair.volume_24h_usd > 0, f"Top pair issue on {pair}"
        assert pair.tvl_latest_usd > 0, f"Top pair issue on {pair}"
        if pair.base_token != "WETH":
            assert pair.token_sniffer_score, f"Top pair issue on {pair}"
            assert pair.token_sniffer_score > 0, f"Top pair issue on {pair}"

