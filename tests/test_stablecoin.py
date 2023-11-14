import datetime

from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import ExchangeType
from tradingstrategy.pair import DEXPair, filter_for_stablecoins, StablecoinFilteringMode
from tradingstrategy.stablecoin import is_stablecoin_like


def test_is_stablecoin_symbol():
    assert is_stablecoin_like("DAI")
    assert is_stablecoin_like("BUSD")
    assert is_stablecoin_like("USDT")
    assert not is_stablecoin_like("BNB")
    assert not is_stablecoin_like("ETH")
    assert not is_stablecoin_like("WBNB")


def test_filter_stablecoin_pairs():

    # Mock some data
    pairs = [
        DEXPair(
            pair_id=1,
            chain_id=ChainId.ethereum,
            exchange_id=1,
            address="0x0000000000000000000000000000000000000000",
            dex_type=ExchangeType.uniswap_v2,
            base_token_symbol="WETH",
            quote_token_symbol="USDC",
            token0_symbol="USDC",
            token1_symbol="WETH",
            token0_address="0x0000000000000000000000000000000000000000",
            token1_address="0x0000000000000000000000000000000000000000",
            first_swap_at_block_number=1,
            last_swap_at_block_number=1,
            first_swap_at=int(datetime.datetime(2020, 6, 4, 11, 42, 39).timestamp()),
            last_swap_at=int(datetime.datetime(2020, 6, 4, 11, 42, 39).timestamp()),
            flag_inactive=False,
            flag_blacklisted_manually=False,
            flag_unsupported_quote_token=False,
            flag_unknown_exchange=False
        ),

        DEXPair(
            pair_id=2,
            chain_id=ChainId.ethereum,
            exchange_id=1,
            address="0x0000000000000000000000000000000000000000",
            dex_type=ExchangeType.uniswap_v2,
            base_token_symbol="DAI",
            quote_token_symbol="USDC",
            token0_symbol="USDC",
            token1_symbol="DAI",
            token0_address="0x0000000000000000000000000000000000000000",
            token1_address="0x0000000000000000000000000000000000000000",
            first_swap_at_block_number=1,
            last_swap_at_block_number=1,
            first_swap_at=int(datetime.datetime(2020, 6, 4, 11, 42, 39).timestamp()),
            last_swap_at=int(datetime.datetime(2020, 6, 4, 11, 42, 39).timestamp()),
            flag_inactive=False,
            flag_blacklisted_manually=False,
            flag_unsupported_quote_token=False,
            flag_unknown_exchange=False
        ),
    ]

    df = DEXPair.convert_to_dataframe(pairs)
    only_stablecoins = filter_for_stablecoins(df, StablecoinFilteringMode.only_stablecoin_pairs)
    assert len(only_stablecoins) == 1
    assert only_stablecoins.iloc[0].token0_symbol == "USDC"
    assert only_stablecoins.iloc[0].token1_symbol == "DAI"

    only_stablecoins = filter_for_stablecoins(df, StablecoinFilteringMode.only_volatile_pairs)
    assert len(only_stablecoins) == 1
    assert only_stablecoins.iloc[0].token0_symbol == "USDC"
    assert only_stablecoins.iloc[0].token1_symbol == "WETH"

