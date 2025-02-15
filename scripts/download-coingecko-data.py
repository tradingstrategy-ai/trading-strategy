"""An example script to obtain the data for the first 1000 coingecko entries. """

import logging
import os
import sys

from tradingstrategy.alternative_data.coingecko import fetch_top_coins, CoingeckoUniverse, CoingeckoClient, DEFAULT_COINGECKO_BUNDLE


def main():
    """Run and save JSON, Zstd compress.

    - Get the first 1000 tokens

    - Get id, metadata (categories) and market cap at the point of time

    - Sort them by market cap

    - Write down Zstd compressed JSON
    """

    logging.basicConfig(handlers=[logging.StreamHandler(sys.stdout)], level=logging.INFO)
    logger = logging.getLogger(__name__)

    total = 3000
    per_page = 60
    pages = total // per_page

    # pages = 1
    # per_page = 5

    logger.info("Starting Coingecko data fetcher to update the default data bundle %s", DEFAULT_COINGECKO_BUNDLE)

    demo = os.environ.get("COINGECKO_DEMO") != "false"
    client = CoingeckoClient(os.environ["COINGECKO_API_KEY"], demo=demo)
    data = fetch_top_coins(client, pages=pages, per_page=per_page)

    universe = CoingeckoUniverse(data)
    logger.info("Coingecko universe is %s", universe)
    logger.info("Coingecko data covers categories: %s", ", ".join(universe.get_all_categories()))
    universe.save()

    print("All ok")

if __name__ == "__main__":
    main()