"""Charting functionality tests."""

import pandas as pd
import pytest
import datetime
from IPython.core.display_functions import display
from pandas.core.groupby import DataFrameGroupBy

from tradingstrategy.chain import ChainId
from tradingstrategy.charting.candle_chart import visualise_ohlcv, make_candle_labels, VolumeBarMode, validate_plot_info
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
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD")]
        )

    pair = pair_universe.get_single()

    # to make it deterministic
    candles: pd.DataFrame = client.fetch_candles_by_pair_ids(
        {pair.pair_id},
        TimeBucket.d1,
        progress_bar_description=f"Download data for {pair.get_ticker()}",
        start_time=datetime.datetime(2023, 1, 1),
        end_time=datetime.datetime(2023, 1, 31),
    )
    
    return candles, pair


def test_candle_chart_volume_overlay(candles_and_pair: tuple[pd.DataFrame, DEXPair]):
    """Draw a candle chart."""
    
    candles, pair = candles_and_pair
    
    fig = visualise_ohlcv(
        candles,
        height=800,
        theme='plotly_white',
        chart_name=f"{pair.base_token_symbol} - {pair.quote_token_symbol} price chart",
        y_axis_name=f"$ {pair.base_token_symbol} price",
        volume_axis_name='Volume USD',
        volume_bar_mode=VolumeBarMode.overlay,
        num_detached_indicators=2,
        vertical_spacing=0.05,
        relative_sizing=None,
        subplot_names=['', 'random 1', 'random 2<br> + random 3<br> + random 4'],
        subplot_font_size=11,
    )
    
     # 3 distinct plot grids
    assert len(fig._grid_ref) == 3
    
    # check the main title
    assert fig.layout.title.text == 'WBNB - BUSD price chart'
    
    # check subplot titles
    subplot_titles = [annotation['text'] for annotation in fig['layout']['annotations']]
    assert subplot_titles[0] == "random 1"
    assert subplot_titles[1] == "random 2<br> + random 3<br> + random 4"
    
    # List of candles, indicators, and markers
    data = fig.to_dict()["data"]
    assert len(data) == 2
    assert data[0]["type"] == "bar"
    assert data[1]["type"] == "candlestick"

    # TODO: How to disable stdout
    # Does not show actually in unit tests, but checks
    # we can render the figure
    # display(figure)
    

def test_candle_chart_volume_hidden(candles_and_pair: tuple[pd.DataFrame, DEXPair]):
    """Draw a candle chart."""
    
    candles, pair = candles_and_pair
    
    fig = visualise_ohlcv(
        candles,
        height=1000,
        theme='plotly_white',
        chart_name=f"{pair.base_token_symbol} - {pair.quote_token_symbol} price chart",
        y_axis_name=f"$ {pair.base_token_symbol} price",
        volume_axis_name='Volume USD',
        volume_bar_mode=VolumeBarMode.hidden,
        num_detached_indicators=3,
        vertical_spacing=0.05,
        relative_sizing=None,
        subplot_names=['', 'random 1', 'random 2', 'random 3'],
        subplot_font_size=5,
    )
    
     # 3 distinct plot grids
    assert len(fig._grid_ref) == 4
    
    # check the main title
    assert fig.layout.title.text == 'WBNB - BUSD price chart'
    
    # check subplot titles
    subplot_titles = [annotation['text'] for annotation in fig['layout']['annotations']]
    assert subplot_titles[0] == "random 1"
    assert subplot_titles[1] == "random 2"
    assert subplot_titles[2] == "random 3"
    
    
    # List of candles, indicators, and markers
    data = fig.to_dict()["data"]
    assert len(data) == 1
    assert data[0]["type"] == "candlestick"
    
    
def test_candle_chart_volume_separate(candles_and_pair: tuple[pd.DataFrame, DEXPair]):
    """Draw a candle chart."""
    
    candles, pair = candles_and_pair
    
    fig = visualise_ohlcv(
        candles,
        height=800,
        theme='plotly_white',
        chart_name=f"{pair.base_token_symbol} - {pair.quote_token_symbol} price chart",
        y_axis_name=f"$ {pair.base_token_symbol} price",
        volume_axis_name='Volume',
        volume_bar_mode=VolumeBarMode.separate,
        num_detached_indicators=2,
        vertical_spacing=0.05,
        relative_sizing=None,
        subplot_names=['', 'volume usd', 'random 1'],
        subplot_font_size=15,
    )
    
     # 3 distinct plot grids
    assert len(fig._grid_ref) == 3
    
    # check the main title
    assert fig.layout.title.text == 'WBNB - BUSD price chart'
    
    # check subplot titles
    subplot_titles = [annotation['text'] for annotation in fig['layout']['annotations']]
    assert subplot_titles[0] == "volume usd"
    assert subplot_titles[1] == "random 1"
    
    # List of candles, indicators, and markers
    data = fig.to_dict()["data"]
    assert len(data) == 2
    assert data[0]["type"] == "bar"
    assert data[1]["type"] == "candlestick"


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

    # Check keys and also 4 decimal places
    assert type(first_label) == str
    assert "Open: 246.4422 USD" in first_label
    assert "Volume: 4770016.1561 USD" in first_label
    assert "Change: -0.96 %" in first_label

    # Try with cryptocurrency based labelling
    labels = make_candle_labels(
        candles,
        dollar_prices=False,
        base_token_name="BNB",
        quote_token_name="BUSD",
        candle_decimals=8,
    )

    # check currencies correct and also 8 decimal places 
    first_label = labels.iloc[0]
    assert "BUSD" in first_label
    assert "BNB" in first_label
    assert "Open: 246.44223377 BNB / BUSD" in first_label
    assert "Close: 244.08475377 BNB / BUSD" in first_label


def test_visualise_with_label(persistent_test_client: Client):
    """Visualise with complex tooltips."""

    client = persistent_test_client
    exchange_universe = client.fetch_exchange_universe()
    pairs_df = client.fetch_pair_universe().to_pandas()

    # Create filtered exchange and pair data
    exchange = exchange_universe.get_by_chain_and_slug(ChainId.bsc, "pancakeswap-v2")
    pair_universe = PandasPairUniverse.create_pair_universe(
            pairs_df,
            [(exchange.chain_id, exchange.exchange_slug, "WBNB", "BUSD"), (exchange.chain_id, exchange.exchange_slug, "Cake", "BUSD")]
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

def testvalidate_plot_info():
    """Test the validation of plot info."""
    
    with pytest.raises(AssertionError):
        validate_plot_info(
            volume_bar_mode=VolumeBarMode.hidden,
            num_detached_indicators=0,
            relative_sizing=None,
            subplot_names=['', 'random 1'],
        )
    
    with pytest.raises(AssertionError):
        validate_plot_info(
            volume_bar_mode=VolumeBarMode.separate,
            num_detached_indicators=1,
            relative_sizing=None,
            subplot_names=['', 'volume', 'should not be here'],
        )
    
    with pytest.raises(AssertionError):
        validate_plot_info(
            volume_bar_mode=VolumeBarMode.overlay,
            num_detached_indicators=0,
            relative_sizing=[1, 0.2],
            subplot_names=None,
        )
    
    with pytest.raises(AssertionError):
        validate_plot_info(
            volume_bar_mode=VolumeBarMode.separate,
            num_detached_indicators=1,
            relative_sizing=[1],
            subplot_names=None,
        )
        
    # provide bad volume_bar_mode
    with pytest.raises(ValueError, match="Invalid volume_bar_mode"):
        validate_plot_info(
            volume_bar_mode='bad',
            num_detached_indicators=0,
            relative_sizing=None,
            subplot_names=None
        )
    
    # check that we can pass validation without providing subplot_names or relative_sizing
    validate_plot_info(
        volume_bar_mode=VolumeBarMode.separate,
        num_detached_indicators=1,
        relative_sizing=None,
        subplot_names=None,
    )