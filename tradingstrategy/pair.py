"""Trading pair information and analysis."""

import enum
from dataclasses import dataclass
from typing import Optional, List, Iterable, Dict

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from dataclasses_json import dataclass_json

from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import ExchangeUniverse, Exchange
from tradingstrategy.types import NonChecksummedAddress, BlockNumber, UNIXTimestamp, BasisPoint, PrimaryKey
from tradingstrategy.utils.columnar import iterate_columnar_dicts
from tradingstrategy.utils.schema import create_pyarrow_schema_for_dataclass, create_columnar_work_buffer, \
    append_to_columnar_work_buffer


class NoPairFound(Exception):
    """No trading pair found matching the given criteria."""


class DuplicatePair(Exception):
    """Found multiple trading pairs for the same naive lookup."""


class PairType(enum.Enum):
    """What kind of an decentralised exchange, AMM or other the pair is trading on.

    Note that each type can have multiple implementations.
    For example QuickSwap, Sushi and Pancake are all Uniswap v2 types.
    """

    #: Uniswap v2 style exchange
    uniswap_v2 = "uni_v2"

    #: Uniswap v3 style exchange
    uniswap_v3 = "uni_v3"


@dataclass_json
@dataclass
class DEXPair:
    """A trading pair information.

    This :term:`dataclass` presents information we have available for a trading pair.
    Note that you do not directly read or manipulate this class, but
    instead use :py:class:`pyarrow.Table` in-memory analytic presentation
    of the data.

    The :term:`dataset server` maintains trading pair and associated token information.
    Some tokens may have more information available than others,
    as due to the high number of pairs it is impractical to get full information
    for all pairs.

    * Non-optional fields are always available

    * Optional fields may be available if the candle server 1) detected the pair popular enough 2) managed to fetch the third party service information related to the token

    .. note ::

        Currently all flags are disabled and will be removed in the future. The historical dataset does not contain any filtering flags,
        because the data has to be filtered prior to download, to keep the download dump in a reasonasble size.
        The current data set of 800k trading pairs produce 100 MB dataset of which most of the pairs
        are useless. The server prefilters trading pairs and thus you cannot access historical data of pairs
        that have been prefiltered.

        For the very same reason, first and last trade data is not available in the client version 0.3 onwards.

    For more information see see :ref:`trading pair tracking <tracking>`.
    """

    #: Internal primary key for any trading pair
    pair_id: PrimaryKey

    #: The chain id on which chain this pair is trading. 1 for Ethereum.
    chain_id: ChainId

    #: The exchange where this token trades
    exchange_id: PrimaryKey

    #: Smart contract address for the pair.
    #: In the case of Uniswap this is the pair (pool) address
    address: NonChecksummedAddress

    #: What kind of exchange this pair is on
    dex_type: PairType

    #: token0 as in raw Uniswap data
    token0_symbol: str

    #: token1 as in raw Uniswap data
    token1_symbol: str

    #: Token pair contract address on-chain
    token0_address: str

    #: Token pair contract address on-chain
    token1_address: str

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

    #: Naturalised base and quote token.
    #: Uniswap may present the pair in USDC-WETH or WETH-USDC order based on the token address order.
    #: However we humans always want the quote token to be USD, or ETH or BTC.
    #: For the reverse token orders, the candle serve swaps the token order
    #: so that the quote token is the more natural token of the pair (in the above case USD)
    base_token_symbol: Optional[str] = None
    quote_token_symbol: Optional[str] = None

    #: Denormalised web page and API look up information
    exchange_slug: Optional[str] = None

    #: Denormalised web page and API look up information
    pair_slug: Optional[str] = None

    #: Block number of the first Uniswap Swap event
    first_swap_at_block_number: Optional[BlockNumber] = None

    #: Block number of the last Uniswap Swap event
    last_swap_at_block_number:  Optional[BlockNumber] = None

    #: Timestamp of the first Uniswap Swap event
    first_swap_at: Optional[UNIXTimestamp] = None

    #: Timestamp of the first Uniswap Swap event
    last_swap_at: Optional[UNIXTimestamp] = None

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
    trustwallet_info: Optional[Dict[str, str]] = None  # TrustWallet database data, as direct dump
    etherscan_info: Optional[Dict[str, str]] = None  # Etherscan pro database data, as direct dump

    #: Risk assessment summary data
    buy_count_all_time: Optional[int] = None

    #: Risk assessment summary data
    sell_count_all_time: Optional[int] = None

    #: Risk assessment summary data
    buy_volume_all_time: Optional[float] = None

    #: Risk assessment summary data
    sell_volume_all_time: Optional[float] = None

    #: Risk assessment summary data
    buy_count_30d: Optional[int] = None

    #: Risk assessment summary data
    sell_count_30d: Optional[int] = None

    #: Risk assessment summary data
    buy_volume_30d: Optional[float] = None

    #: Risk assessment summary data
    sell_volume_30d: Optional[float] = None

    # Uniswap pair on Sushiswap etc.
    same_pair_on_other_exchanges: Optional[List[PrimaryKey]] = None

    # ETH-USDC pair on QuickSwap, PancakeSwap, etc.
    bridged_pair_on_other_exchanges: Optional[List[PrimaryKey]] = None

    # Trading pairs with same token symbol combinations, but no notable volume
    clone_pairs: Optional[List[PrimaryKey]] = None

    def __repr__(self):
        chain_name = self.chain_id.get_slug()
        return f"<Pair #{self.pair_id} {self.base_token_symbol} - {self.quote_token_symbol} ({self.address}) at exchange #{self.exchange_id} on {chain_name}>"

    @property
    def base_token_address(self) -> str:
        """Get smart contract address for the base token"""
        if self.token0_symbol == self.base_token_symbol:
            return self.token0_address
        else:
            return self.token1_address

    @property
    def quote_token_address(self) -> str:
        """Get smart contract address for the quote token"""
        if self.token0_symbol == self.quote_token_symbol:
            return self.token0_address
        else:
            return self.token1_address

    def get_ticker(self) -> str:
        """Return trading 'ticker'"""
        return f"{self.base_token_symbol}-{self.quote_token_symbol}"

    def get_friendly_name(self, exchange_universe: ExchangeUniverse) -> str:
        """Get a very human readable name for this trading pair.

        We need to translate the exchange id to someething human readable,
        and for this we need to have the access to the exchange universe.
        """
        exchange = exchange_universe.get_by_id(self.exchange_id)
        if exchange:
            exchange_name = exchange.name
        else:
            exchange_name = f"Exchange #{self.exchange_id}"
        return f"{self.base_token_symbol} - {self.quote_token_symbol}, pair #{self.pair_id} on {exchange_name}"

    def get_trading_pair_page_url(self) -> Optional[str]:
        """Get information page for this trading pair.

        :return: URL of the trading pair page or None if page/data not available.
        """
        chain_slug = self.chain_id.get_slug()
        if not self.exchange_slug:
            return None
        if not self.pair_slug:
            return None
        return f"https://tradingstrategy.ai/trading-view/{chain_slug}/{self.exchange_slug}/{self.pair_slug}"

    def __json__(self, request):
        """Pyramid JSON renderer compatibility.

        https://docs.pylonsproject.org/projects/pyramid/en/latest/narr/renderers.html#using-a-custom-json-method
        """
        return self.__dict__

    @classmethod
    def to_pyarrow_schema(cls) -> pa.Schema:
        """Construct schema for reading writing :term:`Parquet` filss for pair information."""

        # Enums must be explicitly expressed
        hints = {
            "chain_id": pa.uint16(),
            "dex_type": pa.string(),
        }

        return create_pyarrow_schema_for_dataclass(cls, hints=hints)

    @classmethod
    def convert_to_pyarrow_table(cls, pairs: List["DEXPair"]) -> pa.Table:
        """Convert a list of Python instances to a columnar data.

        :param pairs: The list wil be consumed in the process
        """
        buffer = create_columnar_work_buffer(cls)
        # appender = partial(append_to_columnar_work_buffer, columnar_buffer)
        # map(appender, pairs)

        for p in pairs:
            assert isinstance(p, DEXPair), f"Got {p}"
            append_to_columnar_work_buffer(buffer, p)

        schema = cls.to_pyarrow_schema()

        # field: pa.Field
        # for field in schema:
        #    print("Checking", field.name)
        #    a = pa.array(buffer[field.name], field.type)

        return pa.Table.from_pydict(buffer, schema)


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

    #: Pair info for this universe
    pairs: Dict[int, DEXPair]

    #: When this universe was last refreshed
    last_updated_at: Optional[UNIXTimestamp] = None

    @classmethod
    def create_from_pyarrow_table(cls, table: pa.Table) -> "PairUniverse":
        """Convert columnar presentation to a Python in-memory objects.

        Some data manipulation is easier with objects instead of columns.

        .. note ::

            This seems to quite slow operation.
            It is recommend you avoid this if you do not need row-like data.
        """
        pairs: Dict[int, DEXPair] = {}
        for batch in table.to_batches(max_chunksize=5000):
            d = batch.to_pydict()
            for row in iterate_columnar_dicts(d):
                pairs[row["pair_id"]] = DEXPair.from_dict(row)

        return PairUniverse(pairs=pairs)

    @classmethod
    def create_from_pyarrow_table_with_filters(cls, table: pa.Table, chain_id_filter: Optional[ChainId] = None) -> "PairUniverse":
        """Convert columnar presentation to a Python in-memory objects.

        Filter the pairs based on given filter arguments.
        """

        if chain_id_filter:
            # https://stackoverflow.com/a/64579502/315168
            chain_id_index = table.column('chain_id')
            row_mask = pc.equal(chain_id_index, pa.scalar(chain_id_filter.value, chain_id_index.type))
            selected_table = table.filter(row_mask)

        return PairUniverse.create_from_pyarrow_table(selected_table)

    def get_pair_by_id(self, pair_id: int) -> Optional[DEXPair]:
        """Resolve pair by its id.

        Only useful for debugging. Does a slow look
        """
        return self.pairs[pair_id]

    def get_pair_by_ticker(self, base_token, quote_token) -> Optional[DEXPair]:
        """Get a trading pair by its ticker symbols.

        Note that this method works only very simple universes, as any given pair
        is poised to have multiple tokens and multiple trading pairs on different exchanges.

        :raise DuplicatePair: If the universe contains more than single entry for the pair.

        :return: None if there is no match
        """
        pairs = [p for p in self.pairs.values() if p.base_token_symbol == base_token and p.quote_token_symbol == quote_token]

        if len(pairs) > 1:
            raise DuplicatePair(f"Multiple trading pairs found {base_token}-{quote_token}")

        if pairs:
            return pairs[0]

        return None

    def get_pair_by_ticker_by_exchange(self, exchange_id: int, base_token: str, quote_token: str) -> Optional[DEXPair]:
        """Get a trading pair by its ticker symbols.

        Note that this method works only very simple universes, as any given pair
        is poised to have multiple tokens and multiple trading pairs on different exchanges.

        :param exchange_id: E.g. `1` for uniswap_v2

        :raise DuplicatePair:
            If the universe contains more than single entry for the pair.
            Because we are looking by a token symbol there might be fake tokens with the same symbol.

        :return: None if there is no match
        """

        # Don't let ints slip through as they are unsupported
        assert type(exchange_id) == int

        pairs = [p for p in self.pairs.values()
                 if p.base_token_symbol == base_token
                 and p.quote_token_symbol == quote_token
                 and p.exchange_id == exchange_id]

        if len(pairs) > 1:
            raise DuplicatePair(f"Multiple trading pairs found {base_token}-{quote_token} on exchange {exchange_id}")

        if pairs:
            return pairs[0]

        return None

    def get_all_pairs_on_exchange(self, exchange_id: int) -> Iterable[DEXPair]:
        """Get all trading pair on a decentralsied exchange.

        Use `ExchangeUniverse.get_by_chain_and_slug` to resolve the `exchange_id` first.
        :param chain_id: E.g. `ChainId.ethereum`

        :param exchange_id: E.g. `1` for uniswap_v2

        :raise DuplicatePair:
            If the universe contains more than single entry for the pair.
            Because we are looking by a token symbol there might be fake tokens with the same symbol.

        :return: None if there is no match
        """

        # Don't let ints slip through as they are unsupported
        assert type(exchange_id) == int

        for p in self.pairs.values():
            if p.exchange_id == exchange_id:
                yield p

    def get_active_pairs(self) -> Iterable["DEXPair"]:
        """Filter for pairs that have see a trade for the last 30 days"""
        return filter(lambda p: not p.flag_inactive, self.pairs.values())

    def get_inactive_pairs(self) -> Iterable["DEXPair"]:
        """Filter for pairs that have not see a trade for the last 30 days"""
        return filter(lambda p: p.flag_inactive, self.pairs.values())


class PandasPairUniverse:
    """A pair universe that holds the source data as Pandas dataframe.

    :py:class:`pd.DataFrame` is somewhat more difficult to interact with,
    but offers tighter in-memory presentation for filtering and such.

    The data frame has the same columns as described by :py:class:`DEXPair`.
    """

    def __init__(self, df: pd.DataFrame, build_index=True):
        """
        :param df: The source DataFrame that contains all DEXPair entries

        :param build_index: Build quick lookup index for pairs
        """
        assert isinstance(df, pd.DataFrame)
        self.df = df.set_index(df["pair_id"])

        # pair_id -> DEXPair
        self.pair_map = {}

        # pair smart contract address -> DEXPair
        self.smart_contract_map = {}

        if build_index:
            self.build_index()

    def build_index(self):
        """Create pair_id -> data mapping.

        Allows fast lookup of individual pairs.
        """
        for pair_id, data in self.df.iterrows():
            pair: DEXPair = DEXPair.from_dict(data)
            self.pair_map[pair_id] = pair
            self.smart_contract_map[pair.address.lower()] = pair

    def get_all_pair_ids(self) -> List[PrimaryKey]:
        return self.df["pair_id"].unique()

    def get_count(self) -> int:
        """How many trading pairs there are."""
        return len(self.df)

    def get_unflagged_count(self) -> int:
        """How many trading pairs there are that seem to be legit for analysis.

        TODO: This will be removed in the future releases.
        """
        raise NotImplementedError()

    def get_pair_by_id(self, pair_id: PrimaryKey)  -> Optional[DEXPair]:
        """Look up pair information and return its data.

        :return: Nicely presented :py:class:`DEXPair`.
        """

        if self.pair_map:
            # TODO: Eliminate non-indexed code path?
            return self.pair_map.get(pair_id)

        # TODO: Remove

        df = self.df

        pairs: pd.DataFrame = df.loc[df["pair_id"] == pair_id]

        if len(pairs) > 1:
            raise DuplicatePair(f"Multiple pairs found for id {pair_id}")

        if len(pairs) == 1:
            data = next(iter(pairs.to_dict("index").values()))
            return DEXPair.from_dict(data)

        return None

    def get_pair_by_smart_contract(self, address: str) -> Optional[DEXPair]:
        """Resolve a trading pair by its pool smart contract address.

        :param address: Ethereum smart contract address
        """
        return self.smart_contract_map.get(address)

    def get_single(self) -> DEXPair:
        """For strategies that trade only a single trading pair, get the only pair in the universe.

        :raise AssertionError: If our pair universe does not have an exact single pair
        """
        pair_count = len(self.pair_map)
        assert pair_count == 1, f"Not a single trading pair universe, we have {pair_count} pairs"
        return next(iter(self.pair_map.values()))

    def get_one_pair_from_pandas_universe(self, exchange_id: PrimaryKey, base_token: str, quote_token: str, pick_by_highest_vol=False) -> Optional[DEXPair]:
        """Get a trading pair by its ticker symbols.

        Note that this method works only very simple universes, as any given pair
        is poised to have multiple tokens and multiple trading pairs on different exchanges.

        :param pick_by_highest_vol: If multiple trading pairs with the same symbols are found, pick one with the highest volume. This is because often malicious trading pairs are create to attract novice users.

        :raise DuplicatePair: If the universe contains more than single entry for the pair.

        :return: None if there is no match
        """

        df = self.df

        pairs: pd.DataFrame= df.loc[
            (df["exchange_id"] == exchange_id) &
            (df["base_token_symbol"] == base_token) &
            (df["quote_token_symbol"] == quote_token)]

        if len(pairs) > 1:
            if not pick_by_highest_vol:
                raise DuplicatePair(f"Multiple trading pairs found {base_token}-{quote_token}")

            # Sort by trade volume and pick the highest one
            pairs = pairs.sort_values(by="buy_volume_all_time", ascending=False)
            data = next(iter(pairs.to_dict("index").values()))
            return DEXPair.from_dict(data)

        if len(pairs) == 1:
            data = next(iter(pairs.to_dict("index").values()))
            return DEXPair.from_dict(data)

        return None

    @staticmethod
    def create_single_pair_universe(df: pd.DataFrame, exchange: Exchange, base_token_symbol: str, quote_token_symbol: str) -> "PandasPairUniverse":
        """Create a trading pair universe that contains only a single trading pair.

        :param df: Unfiltered DataFrame for all pairs

        :raise DuplicatePair: Multiple pairs matching the criteria
        :raise NoPairFound: No pairs matching the criteria
        """

        assert exchange is not None, "Got None as Exchange - exchange not found?"

        filtered_df: pd.DataFrame= df.loc[
            (df["exchange_id"] == exchange.exchange_id) &
            (df["base_token_symbol"] == base_token_symbol) &
            (df["quote_token_symbol"] == quote_token_symbol)]

        if len(filtered_df) > 1:
            raise DuplicatePair(f"Multiple trading pairs found {base_token_symbol}-{quote_token_symbol}")

        if len(filtered_df) == 1:
            return PandasPairUniverse(filtered_df)

        raise NoPairFound(f"No trading pair found. Exchange:{exchange} base:{base_token_symbol} quote:{quote_token_symbol}")


def filter_for_exchanges(pairs: pd.DataFrame, exchanges: List[Exchange]) -> pd.DataFrame:
    """Filter dataset so that it only contains data for the trading pairs from a certain exchange.

    Useful as a preprocess step for creating :py:class:`tradingstrategy.candle.GroupedCandleUniverse`
    or :py:class:`tradingstrategy.liquidity.GroupedLiquidityUniverse`.
    """
    exchange_ids = [e.exchange_id for e in exchanges]
    our_pairs: pd.DataFrame = pairs.loc[
        (pairs['exchange_id'].isin(exchange_ids))
    ]
    return our_pairs


def filter_for_quote_tokens(pairs: pd.DataFrame, quote_token_addresses: List[str]) -> pd.DataFrame:
    """Filter dataset so that it only contains data for the trading pairs that have a certain quote tokens.

    Useful as a preprocess step for creating :py:class:`tradingstrategy.candle.GroupedCandleUniverse`
    or :py:class:`tradingstrategy.liquidity.GroupedLiquidityUniverse`.

    You might, for example, want to construct a trading universe where you have only BUSD pairs.

    :param quote_token_addresses: List of Ethereum addresses of the tokens - most be lowercased, as Ethereum addresses in our raw data are.
    """
    assert type(quote_token_addresses) == list

    for addr in quote_token_addresses:
        assert addr == addr.lower(), f"Address was not lowercased {addr}"

    our_pairs: pd.DataFrame = pairs.loc[
        (pairs['token0_address'].isin(quote_token_addresses) | pairs['token1_address'].isin(quote_token_addresses))
    ]
    return our_pairs