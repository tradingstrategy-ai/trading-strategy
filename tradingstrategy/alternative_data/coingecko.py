import logging
import os
from pathlib import Path

from pycoingecko import CoinGeckoAPI


logger = logging.getLogger(__name__)


def create_client(api_key) -> CoinGeckoAPI:
    return CoinGeckoAPI(api_key=api_key)


def fetch_coingecko_coin_list(
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
    return client.get_coins(include_platform=True)


def fetch_coingecko_coin_list_with_market_cap(
    client: CoinGeckoAPI,\
    page=1,
    per_page=50,
):
    """Get data with market cap."""
    assert page > 0
    assert per_page <= 200
    return client.get_coin_markets(page=page, per_page=per_page)



def fetch_category_data(
    client: CoinGeckoAPI,
    pages=40,
    per_page=25,
) -> list[dict]:

    assert isinstance(client, CoinGeckoAPI)

    logger.info("Loading Coingecko id data")

    ids = fetch_coingecko_coin_list(client)
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
            "id_data": id_data,
            "market_cap_data": mcap_entry,
            "metadata": metadata_map[id]
        })

    return result




