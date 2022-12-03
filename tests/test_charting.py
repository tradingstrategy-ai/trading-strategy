"""Charting functionality tests."""

import pandas as pd
from IPython.core.display_functions import display

from tradingstrategy.chain import ChainId
from tradingstrategy.charting.candle_chart import visualise_ohclv
from tradingstrategy.client import Client
from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.timebucket import TimeBucket


def test_candle_chart(persistent_test_client: Client):
    """Draw a candle chart."""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
    pair_universe = PandasPairUniverse.create_single_pair_universe(
            pairs_df,
            exchange,
            "WBNB",
            "BUSD",
            pick_by_highest_vol=True,
        )

    pair = pair_universe.get_single()

    candles: pd.DataFrame = client.fetch_candles_by_pair_ids(
        {pair.pair_id},
        TimeBucket.d1,
        progress_bar_description=f"Download data for {pair.get_ticker()}"
    )

    figure = visualise_ohclv(
        candles,
        chart_name=f"{pair.base_token_symbol} - {pair.quote_token_symbol} price chart",
        y_axis_name=f"$ {pair.base_token_symbol} price",
    )

    # Does not show actually in unit tests, but checks
    # we can render the figure
    display(figure)
