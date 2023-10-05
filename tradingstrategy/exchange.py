"""Exchange information and analysis.

Exchanges are presented by

- :py:class:`Exchange` class

To download the pairs dataset see

- :py:meth:`tradingstrategy.client.Client.fetch_exchange_universe`
"""

import enum
from dataclasses import dataclass
from typing import Optional, List, Iterable, Dict, Collection

from dataclasses_json import dataclass_json

from tradingstrategy.chain import ChainId
from tradingstrategy.types import NonChecksummedAddress, UNIXTimestamp, PrimaryKey
from tradingstrategy.exceptions import DataNotFoundError


class ExchangeNotFoundError(DataNotFoundError):
    """Raised when no exchange found for the given address"""

    template = f"""This might be a problem in your data loading and filtering. 
                
    Use tradingstrategy.ai website to explore DEXs.
    
    Here is a list of DEXes: https://tradingstrategy.ai/trading-view/exchanges
    
    For any further questions join our Discord: https://tradingstrategy.ai/community"""

    def __init__(
            self,
            *, 
            chain_id_name: str = None, 
            exchange_slug: str | None = None, 
            exchange_name: str | None = None, 
            factory_address: str | None = None, 
            exchange_id: int = None, 
            optional_extra_message: str | None = None
        ):
        
        assert exchange_slug or exchange_name or factory_address or exchange_id, "At least one exchange_id, exchange_slug, exchange_name or factory_address must be provided."

        if chain_id_name:
            message = f"The trading universe does not contain data on chain {chain_id_name} for"
        else:
            message = f"The trading universe does not contain data for"

        if exchange_slug:
            message = message + f" exchange_slug {exchange_slug}"

        if exchange_name:
            message = message + f" exchange_name {exchange_name}"

        if factory_address:
            message = message + f" factory_address {factory_address}"
        
        if exchange_id:
            message = message + f" exchange_id {exchange_id}"

        super().__init__(
            f"{message}. {self.template}"
            + (f"\n\n{optional_extra_message}" if optional_extra_message else "")
        )


class ExchangeType(str, enum.Enum):
    """What kind of an decentralised exchange, AMM or other the pair is trading on.

    Note that each type can have multiple implementations.
    For example QuickSwap, Sushi and Pancake are all Uniswap v2 types.
    """

    #: Uniswap v2 style exchange
    uniswap_v2 = "uniswap_v2"

    #: Uniswap v2 style exchange, but with incompatible implementation (e.g. Nomiswap Stable)
    uniswap_v2_incompatible = "uniswap_v2_incompatible"

    #: Uniswap v3 style exchange
    uniswap_v3 = "uniswap_v3"

    # Uniswap v2 style exchange (same as above `uniswap_v2`)
    # NOTE: Do not use this member as it is deprecated and only kept for backward 
    # compatibility, it will be removed in the future
    _deprecated_uni_v2 = "uni_v2"


@dataclass_json
@dataclass
class Exchange:
    """A decentralised exchange.

    Each chain can have multiple active or abadon decentralised exchanges
    of different types, like :term:`AMM` based or order book based.

    The :term:`dataset server` server automatically discovers exchanges
    and tries to add meaningful label and risk data for them.

    Most of the fields are optionally and having values them depends
    on the oracle data indexinb phase.

    Regarding 30d and life time stats like `buy_volume_30d`:
    These stats calculated only if exchanged deemed active and we
    can convert the volume to a supported quote token.
    Any unsupported token volume does not show up in these stats.
    Useful mostly for risk assessment, as this data is **not** accurate,
    but gives some reference information about the popularity of the token.
    """

    #: The chain id on which chain this pair is trading. 1 for Ethereum.
    #: For JSON, this is serialised as one of the name of enum memmbers of ChainId
    #: e.g. `"ethereum"`.
    chain_id: ChainId

    #: The URL slug derived from the blockchain name.
    #: Used as the primary key in URLs and other user facing services.]
    #: Example: "ethereum", "polygon"
    chain_slug: str

    #: The exchange where this token trades
    exchange_id: PrimaryKey

    #: The URL slug derived from the exchange name.
    #: Used as the primary key in URLs and other user facing addressers.
    exchange_slug: str

    #: The factory smart contract address of Uniswap based exchanges.
    address: NonChecksummedAddress

    #: What kind of exchange is this
    exchange_type: ExchangeType

    #: How many pairs we have discovered for this exchange so far
    #: TODO: Make optional - not needed in the tester deployments.
    pair_count: int

    #: How many supported trading pairs we have
    #: See https://tradingstrategy.ai/docs/programming/tracking.html for more information
    active_pair_count: Optional[int] = None

    #: When someone traded on this exchange for the first time
    first_trade_at: Optional[UNIXTimestamp] = None

    #: When someone traded on this exchange last time
    last_trade_at: Optional[UNIXTimestamp] = None

    #: Exchange name - if known or guessed
    name: Optional[str] = None

    #: Exchange homepage if available as https:// link
    homepage: Optional[str] = None

    #: Denormalised exchange statistics
    buy_count_all_time: Optional[int] = None

    #: Denormalised exchange statistics
    sell_count_all_time: Optional[int] = None

    #: Denormalised exchange statistics
    buy_volume_all_time: Optional[float] = None

    #: Denormalised exchange statistics
    sell_volume_all_time: Optional[float] = None

    #: Denormalised exchange statistics
    buy_count_30d: Optional[int] = None

    #: Denormalised exchange statistics
    sell_count_30d: Optional[int] = None

    #: Denormalised exchange statistics
    buy_volume_30d: Optional[float] = None

    #: Denormalised exchange statistics
    sell_volume_30d: Optional[float] = None

    #: The default router address
    #:
    #: If there is a well-known default router implementation like `UniswapV2Router` or `UniswapV3Router` then have its address here.
    #: This cannot be automatically resolved, but the information must be manually managed for every DEX.
    #:
    #: Might not be set.
    default_router_address: Optional[NonChecksummedAddress] = None

    #: Uniswap v2 implementation specific init cod ehash.
    #:
    #: This might be needed in order to route trades.
    #: 0x prefixed hex string of keccak hash.
    #:
    #: #: Might not be set.
    init_code_hash: Optional[str] = None

    def __repr__(self):
        chain_name = self.chain_id.get_name()
        name = self.name or "<unknown>"
        return f"<Exchange {name} at {self.address} on {chain_name}>"

    def __json__(self, request):
        """Pyramid JSON renderer compatibility.

        https://docs.pylonsproject.org/projects/pyramid/en/latest/narr/renderers.html#using-a-custom-json-method
        """
        return self.__dict__

    def __hash__(self) -> int:
        try:
            return int(self.address, 16)
        except TypeError as e:
            raise TypeError(f"Tried to convert: {self.address}") from e

    def __eq__(self, other) -> bool:
        # https://stackoverflow.com/a/12511715/315168
        return isinstance(other, self.__class__) and self.address == other.address

    @property
    def vol_30d(self):
        return (self.buy_volume_30d or 0) + (self.sell_volume_30d or 0)


@dataclass_json
@dataclass
class ExchangeUniverse:
    """Exchange manager.

    Contains look up for exchanges by their internal primary key ids.
    """

    #: Exchange id -> Exchange data mapping
    exchanges: Dict[PrimaryKey, Exchange]

    def __post_init__(self):
        for exchange_id, exchange in self.exchanges.items():
            assert exchange_id == exchange.exchange_id, "Exchange id mismatch"

    @staticmethod
    def from_collection(exchanges: Collection[Exchange]) -> "ExchangeUniverse":
        """Create exchange universe from a collection of exchanges."""
        exchange_dict = {e.exchange_id: e for e in exchanges}
        return ExchangeUniverse(exchanges=exchange_dict)

    def get_by_id(self, id) -> Optional[Exchange]:
        return self.exchanges.get(id)

    def get_exchange_count(self) -> int:
        return len(self.exchanges)

    def get_top_exchanges_by_30d_volume(self) -> List[Exchange]:
        """Get top exchanges sorted by their 30d volume.

        Note that we consider volume only for supported quote tokens.
        See :py:class:`tradingstrategy.exchange.Exchange` for more details.
        """

        def vol(x: Exchange):
            return (x.buy_volume_30d or 0) + (x.sell_volume_30d or 0)

        exchanges = sorted(list(self.exchanges.values()), key=vol, reverse=True)
        return exchanges

    def get_by_chain_and_name(self, chain_id: ChainId, name: str) -> Optional[Exchange]:
        """Get the exchange implementation on a specific chain.

        :param chain_id: Blockchain this exchange is on

        :param name: Like `sushi` or `uniswap v2`. Case insensitive.

        :raises ExchangeNotFoundError: If exchange is not found
        """
        name = name.lower()
        assert isinstance(chain_id, ChainId)
        for xchg in self.exchanges.values():
            if xchg.name.lower() == name.lower() and xchg.chain_id == chain_id:
                return xchg
            
        raise ExchangeNotFoundError(chain_id_name=chain_id.name, exchange_name=name)

    def get_by_chain_and_slug(self, chain_id: ChainId, slug: str) -> Optional[Exchange]:
        """Get the exchange implementation on a specific chain.

        :param chain_id: Blockchain this exchange is on

        :param slug: Machine readable exchange name. Like `uniswap-v2`. Case sensitive.

        :raises ExchangeNotFoundError: If exchange is not found
        """
        assert isinstance(chain_id, ChainId)
        for xchg in self.exchanges.values():
            if xchg.exchange_slug == slug and xchg.chain_id == chain_id:
                return xchg
        
        raise ExchangeNotFoundError(chain_id_name=chain_id.name, exchange_slug=slug)

    def get_by_chain_and_factory(self, chain_id: ChainId, factory_address: str) -> Optional[Exchange]:
        """Get the exchange implementation on a specific chain.

        :param chain_id: Blockchain this exchange is on

        :param factory_address: The smart contract address of the exchange factory
        """
        assert isinstance(chain_id, ChainId)
        factory_address = factory_address.lower()
        for xchg in self.exchanges.values():
            if xchg.address.lower() == factory_address and xchg.chain_id == chain_id:
                return xchg
            
        return ExchangeNotFoundError(chain_id_name=chain_id.name, factory_address=factory_address)

    def get_single(self) -> Exchange:
        """Get the one and the only exchange in this universe.

        :return:
            The exchange

        :raise AssertionError:
            in the case the universe does not contain a single exchange
        """
        assert self.get_exchange_count() == 1
        return next(iter(self.exchanges.values()))

