"""Test /top endpoint."""
import datetime
import itertools

import ipdb

from tradingstrategy.chain import ChainId
from tradingstrategy.client import Client
from tradingstrategy.top import TopPairsReply, TopPairMethod


def test_load_top_by_exchanges(persistent_test_client: Client):
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
        assert pair.get_persistent_string_id() is not None
        assert isinstance(pair.volume_updated_at, datetime.datetime)
        assert isinstance(pair.tvl_updated_at, datetime.datetime)
        assert isinstance(pair.queried_at, datetime.datetime)
        assert pair.volume_24h_usd > 0, f"Top pair issue on {pair}"
        assert pair.tvl_latest_usd > 0, f"Top pair issue on {pair}"
        if pair.base_token != "WETH":
            assert pair.token_sniffer_score, f"Top pair issue on {pair}"
            assert pair.token_sniffer_score > 0, f"Top pair issue on {pair}"


def test_load_top_by_tokens(persistent_test_client: Client):
    """Load top trading pairs by token addresses from /top endpoint.

    - Integration test

    - Inspect TokenSniffer reply data for well known tokens
    """

    client = persistent_test_client

    top_reply = client.fetch_top_pairs(
        chain_ids={ChainId.ethereum},
        addresses={
            "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",  # COMP
            "0xc00e94Cb662C3520282E6f5717214004A7f26888"  # AAVE
        },
        method=TopPairMethod.by_token_addresses,
        limit=None,
    )

    assert isinstance(top_reply, TopPairsReply)
    # The top picks will be COMP/WETH and AAVE/WETH based on volume/liquidity
    assert len(top_reply.included) == 2
    # There are many pairs excluded e.g AAVE/USDC and AAVE/USDT) based ones because of low liq/vol
    assert len(top_reply.excluded) > 0

    comp_weth = top_reply.included[0]
    assert comp_weth.base_token == "COMP"
    assert comp_weth.quote_token == "WETH"
    assert comp_weth.get_buy_tax() == 0
    assert comp_weth.get_sell_tax() == 0
    assert comp_weth.volume_24h_usd > 100.0
    assert comp_weth.tvl_latest_usd > 100.0


def test_token_tax(persistent_test_client: Client):
    """Check the token tax of a token.

    - Get data for 3 taxed token
    """

    client = persistent_test_client

    # Example tokens with tax
    # FRIEND.TECH 0x71fc7cf3e26ce5933fa1952590ca6014a5938138 SCAM
    # $PAAL 0x14feE680690900BA0ccCfC76AD70Fd1b95D10e16
    # TRUMP 0x576e2BeD8F7b46D34016198911Cdf9886f78bea7
    top_reply = client.fetch_top_pairs(
        chain_ids={ChainId.ethereum},
        addresses={
            "0x71fc7cf3e26ce5933fa1952590ca6014a5938138",
            "0x14feE680690900BA0ccCfC76AD70Fd1b95D10e16",
            "0x576e2BeD8F7b46D34016198911Cdf9886f78bea7"
        },
        method=TopPairMethod.by_token_addresses,
    )

    assert isinstance(top_reply, TopPairsReply)
    assert len(top_reply.included) == 2
    assert len(top_reply.excluded) == 2  # FRIEND, another PAAL excluded

    for pair in itertools.chain(top_reply.included, top_reply.excluded):
        if pair.has_tax_data():
            assert 0 < (pair.get_buy_tax() or 0) < 5, f"Pair lacks tax data: {pair}"
            assert 0 < (pair.get_sell_tax() or 0) < 5, f"Pair lacks tax data: {pair}"


