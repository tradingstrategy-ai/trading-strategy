"""Coingecko data fetching and caching.

- Get Coingecko ids, smart contract addresses and categories so we can cross reference
  Trading Strategy data across different vendors

"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict

import zstandard
from pycoingecko import CoinGeckoAPI


logger = logging.getLogger(__name__)


class CoingeckoEntry(TypedDict):
    """CoinGecko data wrapper"""

    #: Response of coin list
    #:
    #: See :py:func:`fetch_coingecko_coin_list`
    #:
    id: dict

    #: Response of market cap
    #:
    #: See :py:func:`fetch_coingecko_coin_list_with_market_cap`
    #:
    market_cap: dict

    #: Response of coin data
    #:
    #: See :py:func:`fetch_coingecko_coin_data`
    #:
    metadata: dict


def create_client(api_key: str, demo=False) -> CoinGeckoAPI:
    """Create pycoingecko client.

    :param demo:
        We are using demo API key
    """
    client = CoinGeckoAPI(api_key=api_key)
    if demo:
        client.api_base_url = "https://api.coingecko.com/api/v3/"
    return client


def fetch_coingecko_coins_list(
    client: CoinGeckoAPI,
) -> list[dict]:
    """Get full Coingecko list of its tokens.

    - Slow API call, not paginated

    Example:

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
    return client.get_coins_list(include_platform=True)


def fetch_coingecko_coin_list_with_market_cap(
    client: CoinGeckoAPI,\
    page=1,
    per_page=50,
):
    """Get data with market cap."""
    assert page > 0
    assert per_page <= 200
    return client.get_coin_markets(page=page, per_page=per_page)



def fetch_coingecko_coin_data(
    client: CoinGeckoAPI,
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

    return client.get_coin_by_id(**params)


def fetch_top_coins(
    client: CoinGeckoAPI,
    pages=40,
    per_page=25,
) -> list[CoingeckoEntry]:
    """Get the list of top coins from CoinGecko, with metadata.

    - Mainly used to built internal database needed for token address matching

    :return:
        Coins sorted by market cap, as dicts.

    """

    assert isinstance(client, CoinGeckoAPI)

    logger.info("Loading Coingecko id data")

    ids = fetch_coingecko_coins_list(client)
    id_map = {id["id"]: id for id in ids}

    market_cap_data = []
    for i in range(pages):
        logger.info("Loading market cap data, page %d", i)
        paginated = fetch_coingecko_coin_list_with_market_cap(client, page=i, per_page=per_page)
        market_cap_data += paginated

    metadata_map = {}
    for mcap_entry in market_cap_data:
        id = mcap_entry["id"]
        logger.info("Loading metadata for %s", id)
        metadata_map[id] = mcap_entry

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
    """

    #: Raw data
    data: list[CoingeckoEntry]

    #: Smart contract address -> entry map
    address_cache: dict[str, CoingeckoEntry]

    #: Coingecko id -> entry map
    id_cache: dict[str, CoingeckoEntry]

    def __init__(self, data: list[CoingeckoEntry]):
        """Create new universe from raw JSON data.

        - Build access indices
        """
        self.data = data

        address_cache = {}
        for entry in data:
            for platform_name, address in entry["platforms"]:
                address_cache[address] = entry

        self.address_cache = address_cache
        self.id_cache = {entry["id"]["id"]: entry for entry in data}

    def __repr__(self):
        return f"<CoingeckoUniverse for {len(self.data)} tokens>"

    def get_by_address(self, address: str) -> CoingeckoEntry | None:
        return self.address_cache.get(address)

    def get_by_coingecko_id(self, id: str) -> CoingeckoEntry | None:
        return self.id_cache.get(id)

    @staticmethod
    def load(fname: Path) -> "CoingeckoUniverse":
        logger.info("Reading Coingecko data bundle to %s", fname)
        with zstandard.open(fname, "rb") as inp:
            data = json.load(inp)
            return CoingeckoUniverse(data)

    def save(self, fname: Path):
        """Creat zstd compressed data file.

        - Save only raw data, no indices
        """
        logger.info("Writing Coingecko data bundle to %s", fname)
        with zstandard.open(fname, "wb") as out:
            json.dump(self.data, out)
