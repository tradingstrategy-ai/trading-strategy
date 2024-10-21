"""An example script to obtain the data for the first 1000 coingecko entries. """

import logging
import os
import sys

from pathlib import Path


from tradingstrategy.alternative_data.coingecko import create_client, fetch_top_coins, CoingeckoUniverse


def main():
    """Run and save JSON, Zstd compress.

    - Get the first 1000 tokens

    - Get id, metadata (categories) and market cap at the point of time

    - Sort them by market cap

    - Write down Zstd compressed JSON
    """

    logging.basicConfig(handlers=[logging.StreamHandler(sys.stdout)], level=logging.INFO)
    logger = logging.getLogger(__name__)

    # pages = 5
    #per_page = 25

    logger.info("Starting Coingecko data fetcher")

    pages = 1
    per_page = 5

    client = create_client(os.environ["COINGECKO_API_KEY"], demo=True)
    data = fetch_top_coins(client, pages=pages, per_page=per_page)
    fname = Path(os.path.join(os.path.dirname(__file__))) / ".." / "trading-strategy", "data_bundles", "coingecko.json.zstd"

    fname = fname.resolve()

    universe = CoingeckoUniverse(data)
    universe.save(fname)

    print("All ok")

if __name__ == "__main__":
    main()