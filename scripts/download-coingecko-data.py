"""An example script to obtain the data for the first 1000 coingecko entries. """
import json
import logging
import os

from pathlib import Path

import zstandard as zstd

from tradingstrategy.alternative_data.coingecko import create_client, fetch_category_data

logger = logging.getLogger(__name__)


def main():
    """Run and save JSON.

    - Get the first 1000 tokens

    - Sort them by market cap
    """

    pages = 5
    per_page = 25

    client = create_client(os.environ["COINGECKO_API_KEY"])
    data = fetch_category_data(client, pages=pages, per_page=per_page)
    fname = Path(os.path.join(os.path.dirname(__file__))) / ".." / "trading-strategy", "data_bundles", "coingecko.json.zstd"

    fname = fname.resolve()
    logger.info("Writing Coingecko data bundle to %s", fname)
    with zstd.open(fname, "wb") as out:
        json.dump(data, out)


if __name__ == "__main__":
    main()