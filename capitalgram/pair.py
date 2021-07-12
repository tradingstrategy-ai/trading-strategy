"""Trading pair information."""
import enum
from dataclasses import dataclass
from typing import Optional, List, Iterable

from dataclasses_json import dataclass_json

from capitalgram.chain import ChainId
from capitalgram.units import NonChecksummedAddress, BlockNumber, UNIXTimestamp, BasisPoint


class DuplicatePair(Exception):
    """Found multiple trading pairs for the same naive lookup."""


class DEXType(enum.Enum):
    """What different DEX, AMM or other on-chain exchanges kinds we support.

    Note that each type can have multiple implementations.
    For example QuickSwap, Sushi and Pancake are all Uniswap v2 types.
    """
    uniswap_v2 = "uni_v2"

    uniswap_v3 = "uni_v3"


class PairUniverse(enum.Enum):
    """Different pair universes available to download"""
    all = "all"


@dataclass_json
@dataclass
class SwapPair:
    """Pair information with risk and diagnostics data attached.

    The candle server maintains token information.
    Some tokens may have more information available than others,
    as due to the high number of pairs it is impractical to get full information
    for all pairs.

    * Non-optional fields are always available

    * Optional fields may be available if the candle server 1) detected the pair popular enough 2) managed to fetch the third party service information related to the token

    Swap pair can be Uniwap v2 like exchange or Uniswap v3 like exchange.
    More pair types coming in the future.
    """
    chain_id: ChainId  # 1 for Ethereum
    address: NonChecksummedAddress  # Pair contract address
    dex_type: DEXType

    #: Naturalised base and quote token.
    #: Uniswap may present the pair in USDC-WETH or WETH-USDC order based on the token address order.
    #: However we humans always want the quote token to be USD, or ETH or BTC.
    #: For the reverse token orders, the candle serve swaps the token order
    #: so that the quote token is the more natural token of the pair (in the above case USD)
    base_token_symbol: str
    quote_token_symbol: str

    #: token0 as in raw Uniswap data
    token0_symbol: str

    #: token1 as in raw Uniswap data
    token1_symbol: str

    #: Token pair contract address on-chain
    token0_address: str

    #: Token pair contract address on-chain
    token1_address: str

    first_swap_at_block_number: BlockNumber
    last_swap_at_block_number: BlockNumber

    first_swap_at: UNIXTimestamp
    last_swap_at: UNIXTimestamp

    #: Pair has been flagged inactive, because it has not traded at least once during the last 30 days.
    flag_inactive: bool

    #: Pair is blacklisted by operators.
    #: Current there is no blacklist process so this is always false.
    flag_blacklisted_manually: bool

    #: Quote token is one of USD, ETH, BTC, MATIC or similar popular token variants.
    #: Because all candle data is outputted in the USD, if we have a quote token
    #: for which we do not have an USD conversation rate reference price source,
    #: we cannot create candles for the pair.
    flag_unsupported_quote_token: bool

    #: Pair is listed on an exchange we do not if it is good or not
    flag_unknown_exchange: bool

    exchange_name: Optional[str] = None  # Exchange name (if known)
    exchange_address: NonChecksummedAddress = None  # Router address in the case of Uniswap v2

    #: Various risk analyis flags
    flag_not_enough_swaps: Optional[bool] = None
    flag_on_trustwallet: Optional[bool] = None
    flag_on_etherscan: Optional[bool] = None
    flag_code_verified: Optional[bool] = None

    #: Swap fee in basis points if known
    fee: Optional[BasisPoint] = None

    trustwallet_info_checked_at: Optional[UNIXTimestamp] = None
    etherscan_info_checked_at: Optional[UNIXTimestamp] = None
    etherscan_code_verified_checked_at: Optional[UNIXTimestamp] = None

    blacklist_reason: Optional[str] = None
    trustwallet_info: Optional[dict] = None  # TrustWallet database data, as direct dump
    etherscan_info: Optional[dict] = None  # Etherscan pro database data, as direct dump

    # Lifetime stats for this pair calculated from daily candles.
    # Only available for active tokens.
    # Useful mostly for risk assessment, as this data is **not** accurate,
    # but gives some reference information about the popularity of the token.
    buy_count_all_time: Optional[int] = None  # Total swaps during the pair lifetime
    sell_count_all_time: Optional[int] = None  # Total swaps during the pair lifetime
    buy_volume_all_time: Optional[float] = None
    sell_volume_all_time: Optional[float] = None
    buy_count_30d: Optional[int] = None
    sell_count_30d: Optional[int] = None
    buy_volume_30d: Optional[float] = None
    sell_volume_30d: Optional[float] = None

    # Uniswap pair on Sushiswap etc.
    same_pair_on_other_exchanges: Optional[list] = None

    # ETH-USDC pair on QuickSwap, PancakeSwap, etc.
    bridged_pair_on_other_exchanges: Optional[list] = None

    # Trading pairs with same token symbol combinations, but no notable volume
    fake_pairs: Optional[list] = None

    def __repr__(self):
        chain_name = self.chain_id.name.capitalize()
        exchange_name = self.exchange_name or "<unknown>"
        return f"<Pair {self.base_token_symbol} - {self.quote_token_symbol} ({self.address}) at exchange {exchange_name} on {chain_name}>"

    def __json__(self, request):
        """Pyramid JSON renderer compatibility"""
        return self.__dict__


@dataclass_json
@dataclass
class PairUniverse:
    """The queries universe, as returned by the server.

    The universe presents tradeable token pairs that
    fulfill certain criteria.

    The server supports different token pair universes
    depending on the risk appetite. As generating the universe
    data is heavy process, the data is generated as a scheduled
    job and cached.

    Risks include

    * Fake tokens designed to fool bots

    * Tokens that may be rug pulls

    * Legit tokens that may have high volatility due to a hack

    * Legit tokens that may stop working in some point

    Depending on your risk apetite, you might want to choose
    between safe and wild west universes.
    """

    #: When this universe was last refreshed
    last_updated_at: UNIXTimestamp

    #: Pair info for this universe
    pairs: List[SwapPair]

    def get_pair_by_ticker(self, base_token, quote_token) -> Optional[SwapPair]:
        """Get a trading pair by its ticker symbols.

        Note that this method works only very simple universes, as any given pair
        is poised to have multiple tokens and multiple trading pairs on different exchanges.

        :raise DuplicatePair: If the universe contains more than single entry for the pair.

        :return: None if there is no match
        """
        pairs = [p for p in self.pairs if p.base_token_symbol == base_token and p.quote_token_symbol == quote_token]

        if len(pairs) > 1:
            raise DuplicatePair(f"Multiple trading pairs found {base_token}-{quote_token}")

        if pairs:
            return pairs[0]

        return None

    def get_active_pairs(self) -> Iterable["SwapPair"]:
        """Filter for pairs that have see a trade for the last 30 days"""
        return filter(lambda p: not p.flag_inactive, self.pairs)

    def get_inactive_pairs(self) -> Iterable["SwapPair"]:
        """Filter for pairs that have not see a trade for the last 30 days"""
        return filter(lambda p: p.flag_inactive, self.pairs)
