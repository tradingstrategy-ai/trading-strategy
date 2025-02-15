"""Coingecko data fetching and caching.

- Get Coingecko ids, smart contract addresses and categories so we can cross reference
  Trading Strategy data across different vendors

- See :py:class:`CoingeckoUniverse` for how to manipulate Coingeck data

- See :py:func:`categorise_pairs` how to label Trading Strategy assets and trading pairs with CoinGecko data
"""

import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict
import datetime

import orjson
import pandas as pd
import requests
from requests import Response
from requests.adapters import HTTPAdapter
import zstandard

from tradingstrategy.chain import ChainId
from tradingstrategy.utils.logging_retry import LoggingRetry
from tradingstrategy.utils.token_filter import add_base_quote_address_columns

logger = logging.getLogger(__name__)


#: Bundled with PyPi distribution for core data
#:
#: File location where we have .json.zstd flat file database within the Python package.
#:
#: Created by :py:func:`fetch_top_coins`
#:
DEFAULT_COINGECKO_BUNDLE = (Path(os.path.dirname(__file__)) / ".." / "data_bundles" / "coingecko.json.zstd").resolve()


class CoingeckoError(Exception):
    """Wrap some Coingecko errors."""

    def __init__(self, msg: str, resp: requests.Response):
        super().__init__(msg)
        self.resp = resp


class CoingeckoUnknownToken(Exception):
    """Coingecko has no data for this token."""


class CoingeckoEntry(TypedDict):
    """Represent one Coingecko coin entry.

    - CoinGecko data wrapper.

    - Typed dict of high level data we collect
    """

    #: Coingecko response of coin id data for the token.
    #:
    #: See :py:func:`fetch_coingecko_coin_list`
    #:
    id: dict

    #: Coingecko response of market cap data for the token.
    #:
    #: See :py:func:`fetch_coingecko_coin_list_with_market_cap`
    #:
    market_cap: dict

    #: Coingecko response of coin data for the token.
    #:
    #: See :py:func:`fetch_coingecko_coin_data`
    #:
    metadata: dict


class CoingeckoClient:
    """Minimal implementation of Coingecko API client."""

    def __init__(self, api_key: str, retries=10, demo=False, timeout=(5, 30)):
        """Create Coingecko client.

        :parma api_key:
            Get from Coingecko.com

        :param retries:
            HTTP request auto retry count.

        :param demo:
            Free Coingecko API keys need to have this flag set.

            Coingecko uses different domain for these requests.
        """
        assert type(api_key) == str
        self.api_key = api_key

        self.session = requests.Session()
        if demo:
            self.session.headers.update({'x-cg-demo-api-key': api_key})
            self.base_url = 'https://api.coingecko.com/api/v3/'
        else:
            self.session.headers.update({'x-cg-pro-api-key': api_key})
            self.base_url = 'https://pro-api.coingecko.com/api/v3/'

        self.session.headers.update({'accept': "application/json"})

        self.session.timeout = timeout

        if retries > 0:
            retry_policy = LoggingRetry(
                total=retries,
                backoff_factor=0.75,
                # TODO: Old urllib3 version?
                # backoff_jitter=4.0,  #  Sleep 0...n seconds
                status_forcelist=[400, 429, 502, 503, 504],
                logger=logger,
            )
            self.session.mount('https://', HTTPAdapter(max_retries=retry_policy))

        logger.info("Coingecko client created, API key: %s..., demo %s", api_key[0:6], demo)

    def make_request(
        self,
        name: str,
        params: dict | None = None,
        raw_response=False,
    ) -> dict | Response:
        """Coingecko JSON get wrapper.

        :raise CoingeckoError:
            Did not get HTTP 200
        """
        url = f"{self.base_url}{name}"

        logger.info("Making Coingecko request %s: %s", url, params)
        resp = self.session.get(url, params=params)

        try:
            resp.raise_for_status()
        except Exception as e:
            if resp.status_code == 429:
                # Throttled.
                # Should not happen as handled by LoggingRetry handler
                logger.info("Coingecko throttling: %s", resp.text)
                raise
            raise CoingeckoError(f"Coingecko error: {resp.status_code} {resp.text}", resp=resp) from e

        if raw_response:
            return resp
        else:
            return resp.json()

    def fetch_coins_list(
        self,
    ) -> list[dict]:
        """Get full Coingecko list of its tokens.

        - Slow API call, not paginated

        Example data:

        .. code-block:: text

            [
              {
                "id": "3a-lending-protocol",
                "symbol": "a3a",
                "name": "3A",
                "platforms": {
                  "ethereum": "0x3f817b28da4940f018c6b5c0a11c555ebb1264f9",
                  "polygon-pos": "0x58c7b2828e7f2b2caa0cc7feef242fa3196d03df",
                  "linea": "0x3d4b2132ed4ea0aa93903713a4de9f98e625a5c7"
                }
              },

              {
                "id": "aave",
                "symbol": "aave",
                "name": "Aave",
                "platforms": {
                  "ethereum": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                  "avalanche": "0x63a72806098bd3d9520cc43356dd78afe5d386d9",
                  "optimistic-ethereum": "0x76fb31fb4af56892a25e32cfc43de717950c9278",
                  "near-protocol": "7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9.factory.bridge.near",
                  "huobi-token": "0x202b4936fe1a82a4965220860ae46d7d3939bb25",
                  "polygon-pos": "0xd6df932a45c0f255f85145f286ea0b292b21c90b",
                  "fantom": "0x6a07a792ab2965c72a5b8088d3a069a7ac3a993b",
                  "harmony-shard-0": "0xcf323aad9e522b93f11c352caa519ad0e14eb40f",
                  "energi": "0xa7f2f790355e0c32cab03f92f6eb7f488e6f049a",
                  "sora": "0x0091bd8d8295b25cab5a7b8b0e44498e678cfc15d872ede3215f7d4c7635ba36",
                  "binance-smart-chain": "0xfb6115445bff7b52feb98650c87f44907e58f802"
                }
              },
              ...

        """
        # https://docs.coingecko.com/v3.0.1/reference/coins-list
        return self.make_request(
            "coins/list",
            {"include_platform": "true"}
        )

    def fetch_coin_markets(
        self,
        page=1,
        per_page=50,
        vs_currency="usd",
    ):
        """Get data with market cap.

        Example:

        .. code-block:: text

            [
              {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "image": "https://coin-images.coingecko.com/coins/images/1/large/bitcoin.png?1696501400",
                "current_price": 67717,
                "market_cap": 1338238834727,
                "market_cap_rank": 1,
                "fully_diluted_valuation": 1421419419213,
                "total_volume": 44825773373,
                "high_24h": 69432,
                "low_24h": 66839,
                "price_change_24h": -1063.1365066438157,
                "price_change_percentage_24h": -1.5457,
                "market_cap_change_24h": -23226732811.611816,
                "market_cap_change_percentage_24h": -1.70601,
                "circulating_supply": 19771093,
                "total_supply": 21000000,
                "max_supply": 21000000,
                "ath": 73738,
                "ath_change_percentage": -8.16696,
                "ath_date": "2024-03-14T07:10:36.635Z",
                "atl": 67.81,
                "atl_change_percentage": 99762.53649,
                "atl_date": "2013-07-06T00:00:00.000Z",
                "roi": null,
                "last_updated": "2024-10-21T21:00:46.632Z"
              },
              {
                "id": "ethereum",
                "symbol": "eth",
                "name": "Ethereum",
                "image": "https://coin-images.coingecko.com/coins/images/279/large/ethereum.png?1696501628",
                "current_price": 2675.55,
                "market_cap": 322048937059,
                "market_cap_rank": 2,
                "fully_diluted_valuation": 322048937059,
                "total_volume": 19681005944,
                "high_24h": 2762.43,
                "low_24h": 2657.49,
                "price_change_24h": -37.684795187311465,
                "price_change_percentage_24h": -1.38892,
                "market_cap_change_24h": -4892365002.109253,
                "market_cap_change_percentage_24h": -1.4964,
                "circulating_supply": 120391113.890841,
                "total_supply": 120391113.890841,
                "max_supply": null,
                "ath": 4878.26,
                "ath_change_percentage": -45.15344,
                "ath_date": "2021-11-10T14:24:19.604Z",
                "atl": 0.432979,
                "atl_change_percentage": 617841.93025,
                "atl_date": "2015-10-20T00:00:00.000Z",
                "roi": {
                  "times": 51.831064806183065,
                  "currency": "btc",
                  "percentage": 5183.106480618307
                },
                "last_updated": "2024-10-21T21:00:48.325Z"
              },

        """
        #      --url 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=25&page=1' \
        assert page > 0, f"Bad page: {page}"
        assert per_page <= 200
        return self.make_request(
            "coins/markets",
            {
                "vs_currency": vs_currency,
                "per_page": str(per_page),
                "page": str(page),
            },
        )

    def fetch_coin_data(
        self,
        id: str,
        localization=False,
        tickers=False,
        market_data=False,
        community_data=True,
        developer_data=True,
        sparkline=False,
    ):
        """Get Coingecko metadata.

        Example:

        .. code-block:: text

            {
              "id": "bitcoin",
              "symbol": "btc",
              "name": "Bitcoin",
              "web_slug": "bitcoin",
              "asset_platform_id": null,
              "platforms": {
                "": ""
              },
              "detail_platforms": {
                "": {
                  "decimal_place": null,
                  "contract_address": ""
                }
              },
              "block_time_in_minutes": 10,
              "hashing_algorithm": "SHA-256",
              "categories": [
                "Cryptocurrency",
                "Layer 1 (L1)",
                "FTX Holdings",
                "Proof of Work (PoW)",
                "Bitcoin Ecosystem",
                "GMCI 30 Index"
              ],
              "preview_listing": false,
              "public_notice": null,
              "additional_notices": [],
              "description": {
                "en": "Bitcoin is the first successful internet money based on peer-to-peer technology; whereby no central bank or authority is involved in the transaction and production of the Bitcoin currency. It was created by an anonymous individual/group under the name, Satoshi Nakamoto. The source code is available publicly as an open source project, anybody can look at it and be part of the developmental process.\r\n\r\nBitcoin is changing the way we see money as we speak. The idea was to produce a means of exchange, independent of any central authority, that could be transferred electronically in a secure, verifiable and immutable way. It is a decentralized peer-to-peer internet currency making mobile payment easy, very low transaction fees, protects your identity, and it works anywhere all the time with no central authority and banks.\r\n\r\nBitcoin is designed to have only 21 million BTC ever created, thus making it a deflationary currency. Bitcoin uses the <a href=\"https://www.coingecko.com/en?hashing_algorithm=SHA-256\">SHA-256</a> hashing algorithm with an average transaction confirmation time of 10 minutes. Miners today are mining Bitcoin using ASIC chip dedicated to only mining Bitcoin, and the hash rate has shot up to peta hashes.\r\n\r\nBeing the first successful online cryptography currency, Bitcoin has inspired other alternative currencies such as <a href=\"https://www.coingecko.com/en/coins/litecoin\">Litecoin</a>, <a href=\"https://www.coingecko.com/en/coins/peercoin\">Peercoin</a>, <a href=\"https://www.coingecko.com/en/coins/primecoin\">Primecoin</a>, and so on.\r\n\r\nThe cryptocurrency then took off with the innovation of the turing-complete smart contract by <a href=\"https://www.coingecko.com/en/coins/ethereum\">Ethereum</a> which led to the development of other amazing projects such as <a href=\"https://www.coingecko.com/en/coins/eos\">EOS</a>, <a href=\"https://www.coingecko.com/en/coins/tron\">Tron</a>, and even crypto-collectibles such as <a href=\"https://www.coingecko.com/buzz/ethereum-still-king-dapps-cryptokitties-need-1-billion-on-eos\">CryptoKitties</a>."
              },
              "links": {
                "homepage": [
                  "http://www.bitcoin.org",
                  "",
                  ""
                ],
                "whitepaper": "https://bitcoin.org/bitcoin.pdf",
                "blockchain_site": [
                  "https://mempool.space/",
                  "https://platform.arkhamintelligence.com/explorer/token/bitcoin",
                  "https://blockchair.com/bitcoin/",
                  "https://btc.com/",
                  "https://btc.tokenview.io/",
                  "https://www.oklink.com/btc",
                  "https://3xpl.com/bitcoin",
                  "",
                  "",
                  ""
                ],
                "official_forum_url": [
                  "https://bitcointalk.org/",
                  "",
                  ""
                ],
                "chat_url": [
                  "",
                  "",
                  ""
                ],
                "announcement_url": [
                  "",
                  ""
                ],
                "twitter_screen_name": "bitcoin",
                "facebook_username": "bitcoins",
                "bitcointalk_thread_identifier": null,
                "telegram_channel_identifier": "",
                "subreddit_url": "https://www.reddit.com/r/Bitcoin/",
                "repos_url": {
                  "github": [
                    "https://github.com/bitcoin/bitcoin",
                    "https://github.com/bitcoin/bips"
                  ],
                  "bitbucket": []
                }
              },
              "image": {
                "thumb": "https://coin-images.coingecko.com/coins/images/1/thumb/bitcoin.png?1696501400",
                "small": "https://coin-images.coingecko.com/coins/images/1/small/bitcoin.png?1696501400",
                "large": "https://coin-images.coingecko.com/coins/images/1/large/bitcoin.png?1696501400"
              },
              "country_origin": "",
              "genesis_date": "2009-01-03",
              "sentiment_votes_up_percentage": 84.79,
              "sentiment_votes_down_percentage": 15.21,
              "watchlist_portfolio_users": 1698440,
              "market_cap_rank": 1,
              "community_data": {
                "facebook_likes": null,
                "twitter_followers": 6894643,
                "reddit_average_posts_48h": 0,
                "reddit_average_comments_48h": 0,
                "reddit_subscribers": 0,
                "reddit_accounts_active_48h": 0,
                "telegram_channel_user_count": null
              },
              "developer_data": {
                "forks": 36426,
                "stars": 73168,
                "subscribers": 3967,
                "total_issues": 7743,
                "closed_issues": 7380,
                "pull_requests_merged": 11215,
                "pull_request_contributors": 846,
                "code_additions_deletions_4_weeks": {
                  "additions": 1570,
                  "deletions": -1948
                },
                "commit_count_4_weeks": 108,
                "last_4_weeks_commit_activity_series": []
              },
              "status_updates": [],
              "last_updated": "2024-10-21T20:20:14.059Z"
            }

        """
        assert type(id) == str

        params = dict(
            localization=localization,
            developer_data=developer_data,
            community_data=community_data,
            market_data=market_data,
            sparkline=sparkline,
            tickers=tickers,
        )

        return self.make_request(
            f"/coins/{id}",
            params,
        )

    def fetch_by_contract(self, chain_id: ChainId, contract_address: str) -> dict:
        """Fetch token data using contract address.

        - Return contract/address Coingecko endpoint as is

        - See https://docs.coingecko.com/reference/coins-contract-address

        :raise CoingeckoUnknownToken:
            If Coingecko gives 404
        """
        assert isinstance(chain_id, ChainId)
        assert isinstance(contract_address, str)
        assert contract_address.startswith("0x")
        blockchain = chain_id.get_coingecko_slug()
        endpoint = f"coins/{blockchain}/contract/{contract_address}"

        try:

            # response = self.session.get(endpoint)
            data = self.make_request(endpoint, params=None)

            if not data:
                raise CoingeckoError("Data returned by Coingecko was empty", resp=response)

            # Add timestamp for caches
            data["queried_at"] = datetime.datetime.utcnow()
            return data
        except CoingeckoError as e:
            if e.resp.status_code == 404:
                raise CoingeckoUnknownToken(f"Token not found on Coingecko: {blockchain}: {contract_address}") from e
            response = e.resp
            raise CoingeckoError(f"Coingecko failure at {endpoint}: {response.status_code} {response.text}", resp=response) from e



def fetch_top_coins(
    client: CoingeckoClient,
    pages=40,
    per_page=25,
) -> list[CoingeckoEntry]:
    """Get the list of top coins from CoinGecko, with metadata.

    - Mainly used to built internal database needed for token address matching

    :return:
        Coins sorted by market cap, as dicts.

    """

    assert isinstance(client, CoingeckoClient)

    logger.info("Loading Coingecko id data")

    ids = client.fetch_coins_list()
    id_map = {id["id"]: id for id in ids}

    market_cap_data = []
    for i in range(pages):
        logger.info("Loading market cap data, page %d", i)
        paginated = client.fetch_coin_markets(page=i+1, per_page=per_page)
        market_cap_data += paginated

    metadata_map = {}
    for idx, mcap_entry in enumerate(market_cap_data, start=1):
        id = mcap_entry["id"]
        logger.info("Loading metadata for %d. %s", idx, id)
        metadata_map[id] = client.fetch_coin_data(id)

    result = []
    for mcap_entry in market_cap_data:
        id = mcap_entry["id"]
        id_data = id_map[id]
        result.append({
            "id": id_data,
            "market_cap": mcap_entry,
            "metadata": metadata_map[id]
        })

    return result


@dataclass
class CoingeckoUniverse:
    """Coingecko data universe.

    - Manage loading and saving Coingecko data in a flat file database

    - Create id and address lookups for tokens

    - We provide a default bundle of first 1000 tokens as :py:attr:`DEFAULT_COINGECKO_BUNDLE`
      included in Trading Strategy PyPi package, sorted by the market cap at the time of creation

    Example usage:

    .. code-block:: python

        # Print out all categories Coingecko has in our default bundle
        from tradingstrategy.alternative_data.coingecko import CoingeckoUniverse

        # Loads the default bundle included in PyPi package
        coingecko_universe = CoingeckoUniverse.load()

        # Prints out all categories found in this bundle.
        # at the time of recording
        categories = sorted(list(coingecko_universe.get_all_categories()), key=str.lower)
        for cat in categories:
            print(cat)

    - Then you can use this universe to build trading pair universe of a specific category,
      see :py:func:`categorise_pairs` for details
    """

    #: Raw data
    data: list[CoingeckoEntry]

    #: Smart contract address -> entry map
    #:
    #: See :py:meth:`get_by_address`
    #:
    address_cache: dict[str, CoingeckoEntry]

    #: Coingecko id -> entry map
    #:
    #: See :py:meth:`get_by_coingecko_id`
    #:
    id_cache: dict[str, CoingeckoEntry]

    #: Category -> list of entries map
    #:
    #: See :py:meth:`get_entries_by_category`
    #:
    category_cache: dict[str, list[CoingeckoEntry]]

    def __init__(self, data: list[CoingeckoEntry]):
        """Create new universe from raw JSON data.

        - Build access indices
        """
        self.data = data

        # Build address cache
        address_cache = {}
        for entry in data:
            for platform_name, address in entry["id"]["platforms"].items():
                address_cache[address.lower()] = entry

        self.address_cache = address_cache

        # Build id cache
        self.id_cache = {entry["id"]["id"]: entry for entry in data}

        # Build category mappings
        category_dict = defaultdict(list)

        # Iterate through the data
        for item in data:
            for category in item["metadata"]['categories']:
                # Add the entire item to the list for this category
                category_dict[category].append(item)

        self.category_cache = category_dict

    def __repr__(self):
        return f"<CoingeckoUniverse for {len(self.data)} tokens>"

    def get_by_address(self, address: str) -> CoingeckoEntry | None:
        """Get entry by its smart contract address.

        :param address:
            E.g. `0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9`.
        """
        return self.address_cache.get(address.lower())

    def get_by_coingecko_id(self, id: str) -> CoingeckoEntry | None:
        """Get entry by its coingecko id.

        :param id:
            E.g. `bitcoin`
        """
        return self.id_cache.get(id)

    def get_all_categories(self) -> set[str]:
        """Get list of Categories in our loade universe.

        See :py:attr:`SAMPLE_CATEGORIES` for an example list
        """
        flat_set = set(tag for item in self.data for tag in item['metadata']["categories"])
        return flat_set

    def get_entries_by_category(self, category: str) -> list[CoingeckoEntry]:
        """Get all tokens under a certain Coingecko category."""
        return self.category_cache.get(category, [])

    @staticmethod
    def load(fname: Path = DEFAULT_COINGECKO_BUNDLE) -> "CoingeckoUniverse":
        """Read JSON + zstd compressed Coingecko flat file database.

        :param fname:
            If not given, use the file bundled in `trading-strategy` package
        """
        logger.info("Reading Coingecko data bundle to %s", fname)
        with zstandard.open(fname, "rb") as inp:
            dump = inp.read()
            data = orjson.loads(dump)
            return CoingeckoUniverse(data)

    def save(self, fname: Path = DEFAULT_COINGECKO_BUNDLE, level=10) -> None:
        """Create JSON + zstd compressed data file for Coingecko tokens.

        - Save only raw data, no indices, which are re-created on read

        :param fname:
            If not given, use the file bundled in `trading-strategy` package

        :param level:
            zstd compression level
        """
        logger.info("Writing Coingecko data bundle to %s", fname)
        with zstandard.open(fname, "wb", cctx=zstandard.ZstdCompressor(level=level)) as out:
            dump = orjson.dumps(self.data)
            out.write(dump)

        logger.info(f"Zstd bundle size is {fname.stat().st_size:,} bytes")


def categorise_pairs(
    coingecko_universe: CoingeckoUniverse,
    pairs_df: pd.DataFrame,
) -> pd.DataFrame:
    """Categorise trading pairs by their Coingecko category.

    - This produces category data for trading pairs (not for assets, same asset can be in multiple trading pairs)

    - Trading pairs are categorised by their base token

    Example:

    .. code-block:: python

        pairs_df = client.fetch_pair_universe().to_pandas()
        coingecko_universe = CoingeckoUniverse.load()
        category_df = categorise_pairs(coingecko_universe, pairs_df)

        # Get pair_ids for all tokens in Coingecko's DeFi category
        mask = category_df["category"] == "Decentralized Finance (DeFi)"
        pair_ids = category_df[mask]["pair_id"]

    See also :py:attr:`SAMPLE_CATEGORIES` for soe example categories.

    See also another example in :py:func:`tradingstrategy.utils.token_filter.deduplicate_pairs_by_volume`.

    :param coingecko_universe:
        Coingecko data bundle.

    :param pairs_df:
        As produced by :py:class:`tradingstrategy.client.Client`

    :return:
        A new dataframe which contains `(pair_id int, address str, category str)` for each token,
        and no index.

        Each pair_id has multiple rows, one for each category where it is contained.
    """

    def _get_categories(address: str) -> list:
        coin_data = coingecko_universe.get_by_address(address)
        if coin_data is None:
            # Our token was not in Coingecko
            return []
        return coin_data["metadata"]["categories"]

    pairs_df = add_base_quote_address_columns(pairs_df)

    category_entries_df = pd.DataFrame({
        "base_token_address": pairs_df["base_token_address"],
        "base_token_symbol": pairs_df["base_token_symbol"],
        "pair_id": pairs_df["pair_id"]
    })
    category_entries_df["category"] = category_entries_df["base_token_address"].apply(_get_categories)
    return category_entries_df.explode("category")



#: An example list of Coingecko categories.
#:
#: One token may belong to multiple categories
#:
SAMPLE_CATEGORIES = [
    'Account Abstraction',
    'Adventure Games',
    'Aelf Ecosystem',
    'AI Agents',
    'AI Meme',
    'Alameda Research Portfolio',
    'Alephium Ecosystem',
    'Algorand Ecosystem',
    'Algorithmic Stablecoin',
    'Alleged SEC Securities',
    'Analytics',
    'Andreessen Horowitz (a16z) Portfolio',
    'Animal Racing',
    'Animoca Brands Portfolio',
    'Appchains',
    'Aptos Ecosystem',
    'Arbitrum Ecosystem',
    'Arbitrum Nova Ecosystem',
    'Archway Ecosystem',
    'Artificial Intelligence (AI)',
    'Asset Manager',
    'Astar Ecosystem',
    'Astar zkEVM Ecosystem',
    'Augmented Reality',
    'Aurora Ecosystem',
    'Automated Market Maker (AMM)',
    'Avalanche Ecosystem',
    'Avalanche L1',
    'Axie Infinity Ecosystem',
    'Base Ecosystem',
    'Base Meme',
    'BEVM Ecosystem',
    'Big Data',
    'Binance HODLer Airdrops',
    'Binance Labs Portfolio',
    'Binance Launchpad',
    'Binance Launchpool',
    'Binance Megadrop',
    'Bitcoin Ecosystem',
    'Bitcoin Fork',
    'Bitcoin Sidechains',
    'Bitgert Ecosystem',
    'BitTorrent Ecosystem',
    'Blast Ecosystem',
    'Blockchain Capital Portfolio',
    'BNB Chain Ecosystem',
    'Boba Network Ecosystem',
    'BRC-20',
    'Breeding',
    'Bridge Governance Tokens',
    'Bridged DAI',
    'Bridged USDC',
    'Bridged USDT',
    'Bridged WBTC',
    'Bridged WETH',
    'BTCfi',
    'Business Platform',
    'Business Services',
    'Canto Ecosystem',
    'Card Games',
    'Cardano Ecosystem',
    'Cat-Themed',
    'Celebrity-Themed',
    'Celo Ecosystem',
    'Centralized Exchange (CEX) Token',
    'ChainGPT Launchpad',
    'Chiliz Ecosystem',
    'Chromia Ecosystem',
    'Circle Ventures Portfolio',
    'Coinbase Ventures Portfolio',
    'Collectibles',
    'Commodity-backed Stablecoin',
    'Communication',
    'Compound Tokens',
    'Conflux Ecosystem',
    'Consensys Portfolio',
    'Core Ecosystem',
    'Cosmos Ecosystem',
    'Cronos Ecosystem',
    'Cross-chain Communication',
    'Crypto-backed Stablecoin',
    'Crypto-Backed Tokens',
    'Cryptocurrency',
    'cToken',
    'Curve Ecosystem',
    'Cyber Ecosystem',
    'Cybersecurity',
    'DaoMaker Launchpad',
    'Data Availability',
    'Decentralized Exchange (DEX)',
    'Decentralized Finance (DeFi)',
    'Decentralized Identifier (DID)',
    'Decentralized Science (DeSci)',
    'DeFiance Capital Portfolio',
    'Delphi Digital Portfolio',
    'DePIN',
    'Derivatives',
    'Dex Aggregator',
    'Discord Bots',
    'Dog-Themed',
    'Dogechain Ecosystem',
    'DragonFly Capital Portfolio',
    'DRC-20',
    'DWF Labs Portfolio',
    'Edgeware Ecosystem',
    'Education',
    'eGirl Capital Portfolio',
    'Elastos Smart Contract Chain Ecosystem',
    'Elon Musk-Inspired',
    'Endurance Ecosystem',
    'Energi Ecosystem',
    'Energy',
    'Entertainment',
    'Eth 2.0 Staking',
    'Ether.fi Ecosystem',
    'Ethereum Ecosystem',
    'EthereumPoW Ecosystem',
    'EUR Stablecoin',
    'Evmos Ecosystem',
    'Exchange-based Tokens',
    'Fan Token',
    'Fantom Ecosystem',
    'Farcaster Ecosystem',
    'Fiat-backed Stablecoin',
    'Finance / Banking',
    'Fixed Interest',
    'Flow Ecosystem',
    'Fractionalized NFT',
    'Frog-Themed',
    'FTX Holdings',
    'Fuse Ecosystem',
    'GalaChain Ecosystem',
    'Galaxy Digital Portfolio',
    'Gambling (GambleFi)',
    'Gaming (GameFi)',
    'Gaming Blockchains',
    'Gaming Governance Token',
    'Gaming Platform',
    'Gaming Utility Token',
    'GMCI 30 Index',
    'GMCI DeFi Index',
    'GMCI DePIN Index',
    'GMCI Layer 1 Index',
    'GMCI Layer 2 Index',
    'GMCI Meme Index',
    'Gnosis Chain Ecosystem',
    'Gotchiverse',
    'Governance',
    'GraphLinq Ecosystem',
    'Guild and Scholarship',
    'Harmony Ecosystem',
    'Healthcare',
    'HECO Chain Ecosystem',
    'Hedera Ecosystem',
    'Huobi ECO Chain Ecosystem',
    'Hybrid Token Standards',
    'Hydra Ecosystem',
    'Hyperliquid Ecosystem',
    'Immutable Ecosystem',
    'Index Coop Defi Index',
    'Infrastructure',
    'Injective Ecosystem',
    'Inscriptions',
    'Insurance',
    'Intent',
    'Internet Computer Ecosystem',
    'Internet of Things (IOT)',
    'Interoperability',
    'IOTA EVM Ecosystem',
    'IoTeX Ecosystem',
    'Kadena Ecosystem',
    'Kaia Ecosystem',
    'KardiaChain Ecosystem',
    'Kaspa Ecosystem',
    'Kava Ecosystem',
    'Kucoin Community Chain Ecosystem',
    'Kujira Ecosystem',
    'Launchpad',
    'Layer 0 (L0)',
    'Layer 1 (L1)',
    'Layer 2 (L2)',
    'Layer 3 (L3)',
    'Legal',
    'Lending/Borrowing',
    'Linea Ecosystem',
    'Liquid Restaked SOL',
    'Liquid Restaking Governance Tokens',
    'Liquid Restaking Tokens',
    'Liquid Staked ETH',
    'Liquid Staked SOL',
    'Liquid Staking',
    'Liquid Staking Governance Tokens',
    'Liquid Staking Tokens',
    'Loopring Ecosystem',
    'LRTfi',
    'LSDFi',
    'Lukso Ecosystem',
    'Manta Network Ecosystem',
    'Mantle Ecosystem',
    'Marketing',
    'Masternodes',
    'Media',
    'Meme',
    'Merlin Chain Ecosystem',
    'Metagovernance',
    'Metaverse',
    'Meter Ecosystem',
    'Metis Ecosystem',
    'MEV Protection',
    'Milkomeda (Cardano) Ecosystem',
    'Mode Ecosystem',
    'Modular Blockchain',
    'Moonbeam Ecosystem',
    'Moonriver Ecosystem',
    'Move To Earn',
    'Multicoin Capital Portfolio',
    'MultiversX Ecosystem',
    'Music',
    'Name Service',
    'Near Protocol Ecosystem',
    'NEO Ecosystem',
    'Neon Ecosystem',
    'NFT',
    'NFT Marketplace',
    'NFTFi',
    'Number',
    'OEC Ecosystem',
    'Ohm Fork',
    'OKT Chain Ecosystem',
    'OKX Ventures Portfolio',
    'Olympus Pro Ecosystem',
    'On-chain Gaming',
    'opBNB Ecocystem',
    'Optimism Ecosystem',
    'Optimism Superchain Ecosystem',
    'Options',
    'Oracle',
    'Oraichain Ecosystem',
    'Osmosis Ecosystem',
    'PAAL AI Launchpad',
    'Pantera Capital Portfolio',
    'Paradigm Portfolio',
    'Parallelized EVM',
    'Parody Meme',
    'Payment Solutions',
    'Perpetuals',
    'Play To Earn',
    'PolitiFi',
    'Polkadot Ecosystem',
    'Polygon Ecosystem',
    'Polygon zkEVM Ecosystem',
    'Poolz Finance Launchpad',
    'Prediction Markets',
    'Presale Meme ',
    'Privacy',
    'Proof of Stake (PoS)',
    'Proof of Work (PoW)',
    'Protocol',
    'Pump.fun Ecosystem',
    'Quest-to-Earn',
    'Racing Games',
    'Radix Ecosystem',
    'Re.al Ecosystem',
    'Real World Assets (RWA)',
    'Rebase Tokens',
    'Regenerative Finance (ReFi)',
    'Restaking',
    'Rollup',
    'Rollups-as-a-Service (RaaS)',
    'Rollux Ecosystem',
    'Ronin Ecosystem',
    'Rootstock Ecosystem',
    'RPG',
    'Runes',
    'RWA Protocol',
    'Sanko Ecosystem',
    'Scroll Ecosystem',
    'Secret Ecosystem',
    'Sei Ecosystem',
    'Seigniorage',
    'Sequoia Capital Portfolio',
    'Shibarium Ecosystem',
    'Shooting Games',
    'SideChain',
    'Simulation Games',
    'Skale Ecosystem',
    'Smart Contract Platform',
    'SocialFi',
    'Software as a service',
    'Solana Ecosystem',
    'Solana Meme',
    'Sora Ecosystem',
    'Sports',
    'Sports Games',
    'Stablecoin Protocol',
    'Stablecoins',
    'Stacks Ecosystem',
    'Starknet Ecosystem',
    'Stellar Ecosystem',
    'Step Network Ecosystem',
    'Storage',
    'Strategy Games',
    'Structured Products',
    'Sui Ecosystem',
    'Sui Meme',
    'Sun Pump Ecosystem',
    'Synthetic Issuer',
    'Syscoin NEVM Ecosystem',
    'Tap to Earn',
    'Technology & Science',
    'Telegram Apps',
    'Telos Ecosystem',
    'Terra Classic Ecosystem',
    'Terra Ecosystem',
    'Tezos Ecosystem',
    'The Boy’s Club',
    'Theta Ecosystem',
    'ThunderCore Ecosystem',
    'TokenFi Launchpad',
    'Tokenized BTC',
    'Tokenized Commodities',
    'Tokenized Gold',
    'Tokenized Real Estate',
    'Tokenized Silver',
    'Tokenized Treasury Bills (T-Bills)',
    'Tokenized Treasury Bonds (T-Bonds)',
    'TON Ecosystem',
    'TON Meme',
    'Tourism',
    'Tron Ecosystem',
    'TRON Meme',
    'USD Stablecoin',
    'Vanar Chain Ecosystem',
    'VeChain Ecosystem',
    'Velas Ecosystem',
    'Venom Ecosystem',
    'Venture Capital Portfolios',
    'Viction Ecocystem',
    'Wall Street Bets Themed',
    'Wallets',
    'WEMIX Ecosystem',
    'World Chain Ecosystem',
    'Wrapped-Tokens',
    'XAI Ecosystem',
    'XDC Ecosystem',
    'XRP Ledger Ecocystem',
    'Yearn Ecosystem',
    'Yield Aggregator',
    'Yield Farming',
    'Yield Optimizer',
    'Yield Tokenization Protocol',
    'Zano Ecosystem',
    'Zedxion Ecosystem',
    'Zero Knowledge (ZK)',
    'ZetaChain Ecosystem',
    'Zilliqa Ecosystem',
    'zkLink Nova Ecosystem',
    'ZkSync Ecosystem',
    'Zoo-Themed',
]
