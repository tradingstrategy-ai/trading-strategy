"""Trading pair information and pair datasets.

The core classes to understand the data are

- :py:class:`DEXPair`

- :py:class:`PandasPairUniverse`

To download the pairs dataset see

- :py:meth:`tradingstrategy.client.Client.fetch_pair_universe`

"""

import logging
import enum
from dataclasses import dataclass
from typing import Optional, List, Iterable, Dict, Union, Set, Tuple

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from dataclasses_json import dataclass_json

from tradingstrategy.chain import ChainId
from tradingstrategy.token import Token
from tradingstrategy.exchange import ExchangeUniverse, Exchange, ExchangeType
from tradingstrategy.stablecoin import ALL_STABLECOIN_LIKE
from tradingstrategy.types import NonChecksummedAddress, BlockNumber, UNIXTimestamp, BasisPoint, PrimaryKey
from tradingstrategy.utils.columnar import iterate_columnar_dicts
from tradingstrategy.utils.schema import create_pyarrow_schema_for_dataclass, create_columnar_work_buffer, \
    append_to_columnar_work_buffer


logger = logging.getLogger(__name__)


class NoPairFound(Exception):
    """No trading pair found matching the given criteria."""


class DuplicatePair(Exception):
    """Found multiple trading pairs for the same naive lookup."""


@dataclass_json
@dataclass
class DEXPair:
    """ Trading pair information for a single pair.

    Presents a single trading pair on decentralised exchanges.

    DEX trading pairs can be uniquely identified by

    - Internal id.

    - (Chain id, address) tuple - the same address can exist on multiple chains.

    - (Chain slug, exchange slug, pair slug) tuple.

    - Token names and symbols are *not* unique - anyone can create any number of trading pair tickers and token symbols.
      Do not rely on token symbols for anything.

    About data:

    - There is a different between `token0` and `token1` and `base_token` and `quote_token` conventions -
      the former are raw DEX (Uniswap) data while the latter are preprocessed by the server to make the data
      more understandable. Base token is the token you are trading and the quote token is the token you consider
      "money" for the trading. E.g. in WETH-USDC, USDC is the quote token. In SUSHI-WETH, WETH is the quote token.

    - Optional fields may be available if the candle server 1) detected the pair popular enough 2) managed to fetch the third party service information related to the token

    When you download a trading pair dataset from the server, not all trading pairs are available.
    For more information about trading pair availability see :ref:`trading pair tracking <tracking>`.

    The class provides some JSON helpers to make it more usable with JSON based APIs.

    This data class is serializable via `dataclasses-json` methods. Example:

    .. code-block::

        info_as_string = pair.to_json()

    You can also do `__json__()` convention data export:

    .. code-block::

        info_as_dict = pair.__json__()

    .. note ::

        Currently all flags are disabled and will be removed in the future. The historical dataset does not contain any filtering flags,
        because the data has to be filtered prior to download, to keep the download dump in a reasonasble size.
        The current data set of 800k trading pairs produce 100 MB dataset of which most of the pairs
        are useless. The server prefilters trading pairs and thus you cannot access historical data of pairs
        that have been prefiltered.

    """

    #: Internal primary key for any trading pair
    pair_id: PrimaryKey

    #: The chain id on which chain this pair is trading. 1 for Ethereum.
    chain_id: ChainId

    #: The exchange where this token trades
    exchange_id: PrimaryKey

    #: Smart contract address for the pair.
    #: In the case of Uniswap this is the pair (pool) address.
    address: NonChecksummedAddress

    #: Token pair contract address on-chain.
    #: Lowercase, non-checksummed.
    token0_address: str

    #: Token pair contract address on-chain
    #: Lowercase, non-checksummed.
    token1_address: str

    #: Token0 as in raw Uniswap data.
    #: ERC-20 contracst are not guaranteed to have this data.
    token0_symbol: Optional[str]

    #: Token1 as in raw Uniswap data
    #: ERC-20 contracst are not guaranteed to have this data.
    token1_symbol: Optional[str]

    #: What kind of exchange this pair is on
    dex_type: Optional[ExchangeType] = None

    #: Naturalised base and quote token.
    #: Uniswap may present the pair in USDC-WETH or WETH-USDC order based on the token address order.
    #: However we humans always want the quote token to be USD, or ETH or BTC.
    #: For the reverse token orders, the candle serve swaps the token order
    #: so that the quote token is the more natural token of the pair (in the above case USD)
    base_token_symbol: Optional[str] = None

    #: Naturalised base and quote token.
    #: Uniswap may present the pair in USDC-WETH or WETH-USDC order based on the token address order.
    #: However we humans always want the quote token to be USD, or ETH or BTC.
    #: For the reverse token orders, the candle serve swaps the token order
    #: so that the quote token is the more natural token of the pair (in the above case USD)
    quote_token_symbol: Optional[str] = None

    #: Number of decimals to convert between human amount and Ethereum fixed int raw amount.
    #: Note - this information might be missing from ERC-20 smart contracts.
    #: If the information is missing the token is not tradeable in practice.
    token0_decimals: Optional[int] = None

    #: Number of decimals to convert between human amount and Ethereum fixed int raw amount
    #: Note - this information might be missing from ERC-20 smart contracts.
    #: If the information is missing the token is not tradeable in practice.
    token1_decimals: Optional[int] = None

    #: Denormalised web page and API look up information
    exchange_slug: Optional[str] = None

    #: Exchange factory address.
    #: Denormalised here, so we do not need an additional lookup.
    exchange_address: Optional[str] = None

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

    #: Pair has been flagged inactive, because it has not traded at least once during the last 30 days.
    #: TODO - inactive, remove.
    flag_inactive: Optional[bool] = None

    #: Pair is blacklisted by operators.
    #: Current there is no blacklist process so this is always false.
    #: TODO - inactive, remove.
    flag_blacklisted_manually: Optional[bool] = None

    #: Quote token is one of USD, ETH, BTC, MATIC or similar popular token variants.
    #: Because all candle data is outputted in the USD, if we have a quote token
    #: for which we do not have an USD conversation rate reference price source,
    #: we cannot create candles for the pair.
    #: TODO - inactive, remove.
    flag_unsupported_quote_token: Optional[bool] = None

    #: Pair is listed on an exchange we do not if it is good or not
    #: TODO - inactive, remove.
    flag_unknown_exchange: Optional[bool] = None

    #: Swap fee in basis points if known
    fee: Optional[BasisPoint] = None

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

    #: Buy token tax for this trading pair.
    #: See :ref:`token-tax` for details.
    buy_tax: Optional[float] = None

    #: Transfer token tax for this trading pair.
    #: See :ref:`token-tax` for details.
    transfer_tax: Optional[float] = None

    #: Sell tax for this trading pair.
    #: See :ref:`token-tax` for details.
    sell_tax: Optional[float] = None

    def __repr__(self):
        chain_name = self.chain_id.get_slug()
        return f"<Pair #{self.pair_id} {self.base_token_symbol} - {self.quote_token_symbol} ({self.address}) at exchange #{self.exchange_id} on {chain_name}>"

    @property
    def base_token_address(self) -> str:
        """Get smart contract address for the base token.

        :return: Lowercase, non-checksummed.
        """
        if self.token0_symbol == self.base_token_symbol:
            return self.token0_address
        else:
            return self.token1_address

    @property
    def quote_token_address(self) -> str:
        """Get smart contract address for the quote token

        :return: Token address in checksummed case
        """
        if self.token0_symbol == self.quote_token_symbol:
            return self.token0_address
        else:
            return self.token1_address

    @property
    def quote_token_decimals(self) -> Optional[str]:
        """Get token decimal count for the quote token"""
        if self.token0_symbol == self.quote_token_symbol:
            return self.token0_decimals
        else:
            return self.token1_decimals

    @property
    def base_token_decimals(self) -> Optional[int]:
        """Get token decimal count for the base token.
        """
        if self.token0_symbol == self.base_token_symbol:
            return self.token0_decimals
        else:
            return self.token1_decimals

    @property
    def quote_token_decimals(self) -> Optional[int]:
        """Get token decimal count for the quote token"""
        if self.token0_symbol == self.quote_token_symbol:
            return self.token0_decimals
        else:
            return self.token1_decimals

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

    def __json__(self, request) -> dict:
        """Return dictionary presentation when this DEXPair is serialised as JSON.

        Provided for pyramid JSON renderer compatibility.

        This method is provided for API endpoints returned

        `More information <https://docs.pylonsproject.org/projects/pyramid/en/latest/narr/renderers.html#using-a-custom-json-method>`_.
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
    def convert_to_pyarrow_table(cls, pairs: List["DEXPair"], check_schema=False) -> pa.Table:
        """Convert a list of DEXPair instances to a Pyarrow table.

        Used to prepare a data export on a server.

        :param pairs: The list wil be consumed in the process

        :param check_schema:
            Run additional checks on the data.
            Slow. Use only in tests. May be give happier
            error messages instead of "OverflowError" what pyarrow spits out.
        """
        buffer = create_columnar_work_buffer(cls)

        for p in pairs:
            assert isinstance(p, DEXPair), f"Got {p}"
            append_to_columnar_work_buffer(buffer, p)

        schema = cls.to_pyarrow_schema()

        if check_schema:
            field: pa.Field
            for field in schema:
                try:
                    pa.array(buffer[field.name], field.type)
                except Exception as e:
                    # Usually cannot fit data into a column, like negative or none values
                    raise RuntimeError(f"Cannot process field {field}") from e

        return pa.Table.from_pydict(buffer, schema)

    @classmethod
    def convert_to_dataframe(cls, pairs: List["DEXPair"]) -> pd.DataFrame:
        """Convert Python DEXPair objects back to the Pandas dataframe presentation.

        As this is super-inefficient, do not use for large amount of data.
        """

        # https://stackoverflow.com/questions/20638006/convert-list-of-dictionaries-to-a-pandas-dataframe
        dicts = [p.to_dict() for p in pairs]
        return pd.DataFrame.from_dict(dicts)


class PandasPairUniverse:
    """A pair universe implementation that is created from Pandas dataset.

    This is a helper class, as :py:class:`pandas.DataFrame` is somewhat more difficult to interact with.
    This class will read the raw data frame and convert it to `DEXPair` objects with a lookup index.
    Because the DEXPair conversion is expensive for 10,000s of Python objects,
    it is recommended that you filter the raw :py:class:`pandas.DataFrame` by using filtering functions
    in :py:mod:`tradingstrategy.pair` first, before initializing :py:class:`PandasPairUniverse`.

    About the usage:

    - Single trading pairs can be looked up using :py:meth:`PandasPairUniverse.get_pair_by_smart_contract`
      and :py:meth:`PandasPairUniverse.get_pair_by_id`

    - Multiple pairs can be looked up by directly reading `PandasPairUniverse.df` Pandas dataframe

    Example how to use:

    .. code-block::

        # Get dataset from the server as Apache Pyarrow table
        columnar_pair_table = client.fetch_pair_universe()

        # Convert Pyarrow -> Pandas -> in-memory DEXPair index
        pair_universe = PandasPairUniverse(columnar_pair_table.to_pandas())

        # Lookup SUSHI-WETH trading pair from DEXPair index
        # https://tradingstrategy.ai/trading-view/ethereum/sushi/sushi-eth
        pair: DEXPair = pair_universe.get_pair_by_smart_contract("0x795065dcc9f64b5614c407a6efdc400da6221fb0")

    If the pair index is too slow to build, or you want to keep it lean,
    you can disable the indexing with `build_index`.
    In this case, some of the methods won't work:

    .. code-block::

        # Get dataset from the server as Apache Pyarrow table
        columnar_pair_table = client.fetch_pair_universe()

        # Convert Pyarrow -> Pandas -> in-memory DEXPair index
        pair_universe = PandasPairUniverse(columnar_pair_table.to_pandas(), build_index=False)

    """

    def __init__(self, df: pd.DataFrame, build_index=True):
        """
        :param df: The source DataFrame that contains all DEXPair entries

        :param build_index: Build quick lookup index for pairs
        """
        assert isinstance(df, pd.DataFrame)
        self.df = df.set_index(df["pair_id"])

        #: pair_id -> DEXPair mappings
        #: Don't access directly, use :py:meth:`iterate_pairs`.
        self.pair_map: Dict[int, dict] = {}

        # pair smart contract address -> DEXPair
        self.smart_contract_map = {}

        # Internal cache for get_token() lookup
        # address -> info tuple mapping
        self.token_cache: Dict[str, Token] = {}

        if build_index:
            self.build_index()

    def iterate_pairs(self) -> Iterable[DEXPair]:
        """Iterate over all pairs in this universe."""
        for p in self.pair_map.values():
            yield DEXPair.from_dict(p)

    def build_index(self):
        """Create pair_id -> data mapping.

        Allows fast lookup of individual pairs.

        .. warning::

            This function assumes the universe contains
            data for only one blockchain. The same address
            can exist across multiple EVM chains.
            The created smart contract address index
            does not index chain id and thus is invalid.

        """
        # https://stackoverflow.com/a/73638890/315168
        self.pair_map = self.df.T.to_dict()
        self.smart_contract_map = {d["address"].lower(): d for d in self.pair_map.values()}

    def get_all_pair_ids(self) -> List[PrimaryKey]:
        return self.df["pair_id"].unique()

    def get_count(self) -> int:
        """How many trading pairs there are."""
        return len(self.df)

    def get_pair_by_id(self, pair_id: PrimaryKey)  -> Optional[DEXPair]:
        """Look up pair information and return its data.

        :return: Nicely presented :py:class:`DEXPair`.
        """

        if self.pair_map:
            # TODO: Eliminate non-indexed code path?
            data = self.pair_map.get(pair_id)
            return DEXPair.from_dict(data)

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

        .. warning::

            This function assumes the universe contains
            data for only one blockchain. The same address
            can exist across multiple EVM chains.

        :param address: Ethereum smart contract address of the Uniswap pair contract
        """
        address = address.lower()
        assert self.smart_contract_map, "You need to build the index to use this function"
        data = self.smart_contract_map.get(address)
        return DEXPair.from_dict(data)

    def get_token(self, address: str) -> Optional[Token]:
        """Get a token that is part of any trade pair.

        Get a token details for a token that is base or quotetoken of any trading pair.

        ..note ::

            TODO: Not a final implementation subject to chage.

        :return:
            Tuple (name, symbol, address, decimals)
            or None if not found.
        """
        address = address.lower()

        token: Optional[Token] = None

        if address not in self.token_cache:
            for pair_data in self.pair_map.values():
                if pair_data["token0_address"] == address:
                    p = DEXPair.from_dict(pair_data)
                    token = Token(p.chain_id, p.token0_symbol, p.token0_address, p.token0_decimals)
                    break
                elif pair_data["token1_address"] == address:
                    p = DEXPair.from_dict(pair_data)
                    token = Token(p.chain_id, p.token1_symbol, p.token1_address, p.token1_decimals)
                    break
            self.token_cache[address] = token
        return self.token_cache[address]

    def get_all_tokens(self) -> Set[Token]:
        """Get all base and quote tokens in trading pairs.

        .. warning ::

            This method is useful for only test/limited pair count universes.
            It is very slow and mainly purported for debugging and diagnostics.

        """
        tokens = set()
        for pair_data in self.pair_map.values():
            p = DEXPair.from_dict(pair_data)
            tokens.add(self.get_token(p.base_token_address))
            tokens.add(self.get_token(p.quote_token_address))
        return tokens

    def get_single(self) -> DEXPair:
        """For strategies that trade only a single trading pair, get the only pair in the universe.

        :raise AssertionError: If our pair universe does not have an exact single pair
        """
        pair_count = len(self.pair_map)
        assert pair_count == 1, f"Not a single trading pair universe, we have {pair_count} pairs"
        return DEXPair.from_dict(next(iter(self.pair_map.values())))

    def get_by_symbols(self, base_token_symbol: str, quote_token_symbol: str) -> Optional[DEXPair]:
        """For strategies that trade only a few trading pairs, get the only pair in the universe.

        .. warning ::

            Currently, this method is only safe for prefiltered universe. There are no safety checks if
            the returned trading pair is legit. In the case of multiple matching pairs,
            a random pair is returned.g

        """
        for pair in self.pair_map.values():
            if pair["base_token_symbol"] == base_token_symbol and pair["quote_token_symbol"] == quote_token_symbol:
                return DEXPair.from_dict(pair)
        return None

    def get_one_pair_from_pandas_universe(self, exchange_id: PrimaryKey, base_token: str, quote_token: str, pick_by_highest_vol=False) -> Optional[DEXPair]:
        """Get a trading pair by its ticker symbols.

        Note that this method works only very simple universes, as any given pair
        is poised to have multiple tokens and multiple trading pairs on different exchanges.

        Example:

        .. code-block:: python

            # Get PancakeSwap exchange,
            # for the full exchange list see https://tradingstrategy.ai/trading-view/exchanges
            pancake = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")

            # Because there can be multiple trading pairs with same tickers,
            # we pick the genuine among the scams based on its trading volume
            wbnb_busd_pair = pair_universe.get_one_pair_from_pandas_universe(
                pancake.exchange_id,
                "WBNB",
                "BUSD",
                pick_by_highest_vol=True,
                )

            print("WBNB address is", wbnb_busd_pair.base_token_address)
            print("BUSD address is", wbnb_busd_pair.quote_token_address)
            print("WBNB-BUSD pair contract address is", wbnb_busd_pair.address)

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
                for p in pairs.to_dict(orient="records"):
                    logger.error("Conflicting pair: %s", p)
                raise DuplicatePair(f"Found {len(pairs)} trading pairs for {base_token}-{quote_token} when 1 was expected")

            # Sort by trade volume and pick the highest one
            pairs = pairs.sort_values(by="buy_volume_all_time", ascending=False)
            data = next(iter(pairs.to_dict("index").values()))
            return DEXPair.from_dict(data)

        if len(pairs) == 1:
            data = next(iter(pairs.to_dict("index").values()))
            return DEXPair.from_dict(data)

        return None

    def create_parquet_load_filter(self, count_limit=10_000) -> List[Tuple]:
        """Returns a Parquet loading filter that contains pairs in this universe.

        When candle or liquidity file is read to the memory,
        only read pairs that are within this pair universe.
        This severely reduces the memory usage and speed ups loading.

        See :py:func:`tradingstrategy.reader.read_parquet`.

        :param count_limit:
            Sanity check assert limit how many pairs we can cram into the filter.

        :return:
            Filter to be passed to read_table
        """

        count = self.get_count()
        assert count < count_limit, f"Too many pairs to create a filter. Pair count is {count}"

        # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html
        return [("pair_id", "in", self.get_all_pair_ids())]

    @staticmethod
    def create_single_pair_universe(
            df: pd.DataFrame,
            exchange: Exchange,
            base_token_symbol: str,
            quote_token_symbol: str,
            pick_by_highest_vol=True) -> "PandasPairUniverse":
        """Create a trading pair universe that contains only a single trading pair.

        This is useful for trading strategies that to technical analysis trading
        on a single trading pair like BTC-USD.

        :param df:
            Unfiltered DataFrame for all pairs

        :param exchange:
            Exchange instance on the pair is trading

        :param base_token_symbol:
            Base token symbol of the trading pair

        :param quote_token_symbol:
            Quote token symbol of the trading pair

        :param pick_by_highest_vol:
            In the case of multiple match per token symbol,
            or scam tokens,
            pick one with the highest trade volume

        :raise DuplicatePair: Multiple pairs matching the criteria
        :raise NoPairFound: No pairs matching the criteria
        """
        return PandasPairUniverse.create_limited_pair_universe(
            df,
            exchange,
            [(base_token_symbol, quote_token_symbol)],
            pick_by_highest_vol,
        )

    @staticmethod
    def create_limited_pair_universe(
            df: pd.DataFrame,
            exchange: Exchange,
            pairs: List[Tuple[str, str]],
            pick_by_highest_vol=True) -> "PandasPairUniverse":
        """Create a trading pair universe that contains only few trading pairs.

        This is useful for trading strategies that to technical analysis trading
        on a few trading pairs, or single pair three-way trades like Cake-WBNB-BUSD.

        :param df:
            Unfiltered DataFrame for all pairs

        :param exchange:
            Exchange instance on the pair is trading

        :param pairs:
            List of trading pairs as ticket tuples. E.g. `[ ("WBNB, "BUSD"), ("Cake", "WBNB") ]`

        :param pick_by_highest_vol:
            In the case of multiple match per token symbol,
            or scam tokens,
            pick one with the highest trade volume

        :raise DuplicatePair: Multiple pairs matching the criteria
        :raise NoPairFound: No pairs matching the criteria
        """

        assert exchange is not None, "Got None as Exchange - exchange not found?"

        # https://pandas.pydata.org/docs/user_guide/merging.html
        frames = []

        for base_token_symbol, quote_token_symbol in pairs:

            filtered_df: pd.DataFrame= df.loc[
                (df["exchange_id"] == exchange.exchange_id) &
                (df["base_token_symbol"] == base_token_symbol) &
                (df["quote_token_symbol"] == quote_token_symbol)]

            if len(filtered_df) > 1:
                if not pick_by_highest_vol:
                    duplicates = 0
                    for p in filtered_df.to_dict(orient="records"):
                        logger.error("Conflicting pair: %s", p)
                        duplicates += 1
                    raise DuplicatePair(f"Found {duplicates} trading pairs for {base_token_symbol}-{quote_token_symbol} when 1 was expected")

                # Sort by trade volume and pick the highest one
                sorted = filtered_df.sort_values(by="buy_volume_all_time", ascending=False)
                duplicates_removed_df = sorted.drop_duplicates(subset="base_token_symbol")
                frames.append(duplicates_removed_df)

            elif len(filtered_df) == 1:
                frames.append(filtered_df)

            else:
                raise NoPairFound(f"No trading pair found. Exchange:{exchange} base:{base_token_symbol} quote:{quote_token_symbol}")

        return PandasPairUniverse(pd.concat(frames))


class LegacyPairUniverse:
    """The queries universe, as returned by the server.

    .. note ::

        TODO: Legacy prototype implementation and will be deprecated.

    Converts raw pair dataset to easier to use `DEXPair`
    in-memory index.

    You likely want to use :py:class:`PandasPairUniverse`,
    as its offers much more functionality than this implemetation.
    """

    #: Internal id -> DEXPair mapping
    pairs: Dict[int, DEXPair]

    def __init__(self, pairs: Dict[int, DEXPair]):
        self.pairs = pairs

    @classmethod
    def create_from_pyarrow_table(cls, table: pa.Table) -> "LegacyPairUniverse":
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

        return LegacyPairUniverse(pairs=pairs)

    @classmethod
    def create_from_pyarrow_table_with_filters(cls, table: pa.Table, chain_id_filter: Optional[ChainId] = None) -> "LegacyPairUniverse":
        """Convert columnar presentation to a Python in-memory objects.

        Filter the pairs based on given filter arguments.
        """

        if chain_id_filter:
            # https://stackoverflow.com/a/64579502/315168
            chain_id_index = table.column('chain_id')
            row_mask = pc.equal(chain_id_index, pa.scalar(chain_id_filter.value, chain_id_index.type))
            selected_table = table.filter(row_mask)

        return LegacyPairUniverse.create_from_pyarrow_table(selected_table)

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


def filter_for_quote_tokens(pairs: pd.DataFrame, quote_token_addresses: Union[List[str], Set[str]]) -> pd.DataFrame:
    """Filter dataset so that it only contains data for the trading pairs that have a certain quote tokens.

    Useful as a preprocess step for creating :py:class:`tradingstrategy.candle.GroupedCandleUniverse`
    or :py:class:`tradingstrategy.liquidity.GroupedLiquidityUniverse`.

    You might, for example, want to construct a trading universe where you have only BUSD pairs.

    :param quote_token_addresses: List of Ethereum addresses of the tokens - most be lowercased, as Ethereum addresses in our raw data are.
    """
    assert type(quote_token_addresses) in (list, set), f"Received: {type(quote_token_addresses)}: {quote_token_addresses}"

    for addr in quote_token_addresses:
        assert addr == addr.lower(), f"Address was not lowercased {addr}"

    our_pairs: pd.DataFrame = pairs.loc[
        (pairs['token0_address'].isin(quote_token_addresses) & (pairs['token0_symbol'] == pairs['quote_token_symbol'])) |
        (pairs['token1_address'].isin(quote_token_addresses) & (pairs['token1_symbol'] == pairs['quote_token_symbol']))
    ]

    return our_pairs


class StablecoinFilteringMode(enum.Enum):
    """How to filter pairs in stablecoin filtering."""
    only_stablecoin_pairs = "only_stablecoin_pairs"
    only_volatile_pairs = "only_volatile_pairs"


def filter_for_stablecoins(pairs: pd.DataFrame, mode: StablecoinFilteringMode) -> pd.DataFrame:
    """Filter dataset so that it only contains data for the trading pairs that are either stablecoin pairs or not.

    Trading logic might not be able to deal with or does not want to deal with stable -> stable pairs.
    Trading stablecoin to another does not make sense, unless you are doing high volume arbitration strategies.

    Uses internal stablecoin list from :py:mod:`tradingstrategy.stablecoin`.
    """
    assert isinstance(mode, StablecoinFilteringMode)

    if mode == StablecoinFilteringMode.only_stablecoin_pairs:
        our_pairs: pd.DataFrame = pairs.loc[
            (pairs['token0_symbol'].isin(ALL_STABLECOIN_LIKE) & pairs['token1_symbol'].isin(ALL_STABLECOIN_LIKE))
        ]
    else:
        # https://stackoverflow.com/a/35939586/315168
        our_pairs: pd.DataFrame = pairs.loc[
            ~(pairs['token0_symbol'].isin(ALL_STABLECOIN_LIKE) & pairs['token1_symbol'].isin(ALL_STABLECOIN_LIKE))
        ]
    return our_pairs

def resolve_pairs_based_on_ticker(
        df: pd.DataFrame,
        chain_id: ChainId,
        exchange_slug: str,
        pair_tickers: Set[Tuple[str, str]],
) -> pd.DataFrame:
    """Resolve symbolic trading pairs to their internal integer primary key ids.

    Uses pair database described :py:class:`DEXPair` Pandas dataframe
    to resolve pairs to their integer ids on a single exchange.

    .. warning ::

        For popular trading pairs, there will be multiple scam pairs
        with the same ticker name. In this case, one with the highest all-time
        buy volume is chosen.

    .. note ::

        Pair ids are not stable and may change long term.
        Always resolve pair ids before a run.

    Example:

    .. code-block: python

        pairs_df = client.fetch_pair_universe().to_pandas()

        tickers = {
            ("WBNB", "BUSD"),
            ("Cake", "WBNB"),
        }

        # ticker -> pd.Series row map for pairs
        filtered_pairs_df = resolve_pairs_based_on_ticker(
            pairs_df,
            ChainId.bsc,
            "pancakeswap-v2",
            tickers
        )

        assert len(filtered_pairs_df) == 2
        wbnb_busd = filtered_pairs_df.loc[
            (filtered_pairs_df["base_token_symbol"] == "WBNB") &
            (filtered_pairs_df["quote_token_symbol"] == "BUSD")
        ].iloc[0]
        assert wbnb_busd["buy_volume_30d"] > 0

    :param df:
        DataFrame containing DEXPairs

    :param chain_id:
        Blockchain the exchange is on

    :param exchange_slug:
        Symbolic exchange name

    :param pair_tickers:
        List of trading pairs as (base token, quote token) tuples.
        Note that giving trading pair tokens in wrong order
        causes pairs not to be found.
        If any ticker does not match it is not included in the result set.

    :return:
        DataFrame with filtered pairs.
    """

    # Create list of conditions to filter out dataframe,
    # one condition per pair
    conditions = []
    for base, quote in pair_tickers:
        condition = (df["base_token_symbol"].str.lower() == base.lower()) & \
                    (df["quote_token_symbol"].str.lower() == quote.lower())
        conditions.append(condition)

    # OR call conditions together
    # https://stackoverflow.com/a/57468610/315168
    df_matches = df.loc[np.logical_or.reduce(conditions)]

    # Sort by the buy volume as a preparation
    # for the scam filter
    df_matches = df_matches.sort_values(by="buy_volume_all_time", ascending=False)

    result_pair_ids = set()

    # Scam filter.
    # Pick the tokens by the highest buy volume to the result map.
    for test_pair in pair_tickers:
        for idx, row in df_matches.iterrows():
            if row["base_token_symbol"] == test_pair[0] and \
               row["quote_token_symbol"] == test_pair[1] and \
               row["exchange_slug"] == exchange_slug:
                    result_pair_ids.add(row["pair_id"])
                    break

    result_df = df.loc[df["pair_id"].isin(result_pair_ids)]
    return result_df
