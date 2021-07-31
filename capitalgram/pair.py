"""Trading pair information and analysis."""

import enum
from dataclasses import dataclass
from functools import partial
from typing import Optional, List, Iterable, Dict

import pandas as pd
import pyarrow as pa
from dataclasses_json import dataclass_json

from capitalgram.chain import ChainId
from capitalgram.types import NonChecksummedAddress, BlockNumber, UNIXTimestamp, BasisPoint, PrimaryKey
from capitalgram.utils.columnar import iterate_columnar_dicts
from capitalgram.utils.schema import create_pyarrow_schema_for_dataclass, create_columnar_work_buffer, \
    append_to_columnar_work_buffer


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
        chain_name = self.chain_id.name.capitalize()
        return f"<Pair {self.base_token_symbol} - {self.quote_token_symbol} ({self.address}) at exchange #{self.exchange_id} on {chain_name}>"

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

    def __init__(self, df: pd.DataFrame):
        """
        :param df: The source DataFrame that contains all DEXPair entries
        """
        assert isinstance(df, pd.DataFrame)
        self.df = df

    def get_all_pair_ids(self) -> List[PrimaryKey]:
        return self.df["pair_id"].unique()

    def get_pair_by_id(self, pair_id: PrimaryKey)  -> Optional[DEXPair]:
        """Look up pair information and return its data.

        :return: Nicely presented :py:class:`DEXPair`.
        """

        df = self.df

        pairs: pd.DataFrame= df.loc[df["pair_id"] == pair_id]

        if len(pairs) > 1:
            raise DuplicatePair(f"Multiple pairs found for id {pair_id}")

        if len(pairs) == 1:
            data = next(iter(pairs.to_dict("index").values()))
            return DEXPair.from_dict(data)

        return None

    def get_one_pair_from_pandas_universe(self, exchange_id: PrimaryKey, base_token: str, quote_token: str) -> Optional[DEXPair]:
        """Get a trading pair by its ticker symbols.

        Note that this method works only very simple universes, as any given pair
        is poised to have multiple tokens and multiple trading pairs on different exchanges.

        :raise DuplicatePair: If the universe contains more than single entry for the pair.

        :return: None if there is no match
        """

        df = self.df

        pairs: pd.DataFrame= df.loc[
            (df["exchange_id"] == exchange_id) &
            (df["base_token_symbol"] == base_token) &
            (df["quote_token_symbol"] == quote_token)]

        if len(pairs) > 1:
            raise DuplicatePair(f"Multiple trading pairs found {base_token}-{quote_token}")

        if len(pairs) == 1:
            data = next(iter(pairs.to_dict("index").values()))
            return DEXPair.from_dict(data)

        return None
