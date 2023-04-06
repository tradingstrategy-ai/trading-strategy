"""Charting functionality tests."""

import pandas as pd
import pytest
from IPython.core.display_functions import display
from pandas.core.groupby import DataFrameGroupBy

from tradingstrategy.chain import ChainId
from tradingstrategy.charting.candle_chart import visualise_ohlcv, make_candle_labels
from tradingstrategy.client import Client
from tradingstrategy.pair import PandasPairUniverse, DEXPair
from tradingstrategy.timebucket import TimeBucket


@pytest.fixture(scope="module")
def candles_and_pair(persistent_test_client: Client) -> tuple[pd.DataFrame, DEXPair]:
    """Get candles and pair for testing."""
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
    
    return candles, pair


def test_candle_chart(candles_and_pair: tuple[pd.DataFrame, DEXPair]):
    """Draw a candle chart."""

    
    candles, pair = candles_and_pair
    
    figure = visualise_ohlcv(
        candles,
        chart_name=f"{pair.base_token_symbol} - {pair.quote_token_symbol} price chart",
        y_axis_name=f"$ {pair.base_token_symbol} price",
    )

    # TODO: How to disable stdout
    # Does not show actually in unit tests, but checks
    # we can render the figure
    # display(figure)


def test_candle_labels(candles_and_pair: tuple[pd.DataFrame, DEXPair]):
    """Create labels for a candle chart."""

    candles, pair = candles_and_pair

    assert "label" not in candles.columns

    # Try with US dollar based labeling
    labels = make_candle_labels(
        candles,
        dollar_prices=True,
        base_token_name=None,
        quote_token_name=None)

    first_label = labels.iloc[0]

    assert type(first_label) == str
    assert "Open:" in first_label
    assert "Volume:" in first_label
    assert "Change:" in first_label

    # Try with cryptocurrency based labelling
    labels = make_candle_labels(
        candles,
        dollar_prices=False,
        base_token_name="BNB",
        quote_token_name="BUSD")

    first_label = labels.iloc[0]
    assert "BUSD" in first_label
    assert "BNB" in first_label


def test_visualise_with_label(persistent_test_client: Client):
    """Visualise with complex tooltips."""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
    pair_universe = PandasPairUniverse.create_limited_pair_universe(
            pairs_df,
            exchange,
            [("WBNB", "BUSD"), ("Cake", "BUSD"),],
            pick_by_highest_vol=True,
        )

    # Create a set of pairs
    pairs = pair_universe.get_all_pair_ids()

    candles: pd.DataFrame = client.fetch_candles_by_pair_ids(
        set(pairs),
        TimeBucket.d1,
        progress_bar_description=f"Download data for multiple pairs"
    )

    grouped_df = candles.groupby("pair_id")
    assert isinstance(grouped_df, DataFrameGroupBy)

    # Try with US dollar based labeling
    pair_df = grouped_df.get_group(pairs[0])
    labels = make_candle_labels(
        pair_df,
        dollar_prices=True,
        base_token_name=None,
        quote_token_name=None)

    first_label = labels.iloc[0]
    assert "Open:" in first_label

    visualise_ohlcv(
        candles,
        labels=labels
    )
