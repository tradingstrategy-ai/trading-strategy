"""Drawing OHLCV candle charts using Plotly.

Draw price charts using Plotly.

- Candle charts

- Open, high, low, close (OHLC)

- Open, high, low, close, volume (OHLCV)

- Dark and light themes
"""
import enum
import logging
from typing import Optional

import plotly.graph_objects as go
import pandas as pd
from plotly.subplots import make_subplots


logger = logging.getLogger(__name__)


class BadOHLCVData(Exception):
    """We could not figure out the data frame"""


class VolumeBarMode(enum.Enum):
    """Should candlestick chart come with the volume bars."""

    #: Volume bars are in a chart below candle chart
    separate = "separate"

    #: Volume bars are transparently inside the candle chart
    overlay = "overlay"

    #: Do not show volume bars
    hidden = "hidden"


def validate_ohclv_dataframe(candles: pd.DataFrame):
    """Ensures that ohclv dataframe contains valid data:
    1. Must either contain an index or column named date or timestamp
    2. Must contain columns named open, close, high, and low"""

    required_columns = ["open", "close", "high", "low"]

    if (
        candles.index.name not in {"date", "timestamp"} and \
        not ({"date", "timestamp"}).issubset(candles.columns)
    ):
        raise BadOHLCVData("OHLCV DataFrame lacks date/timestamp index or column")

    for r in required_columns:
        if r not in candles.columns:
            raise BadOHLCVData(f"OHLCV DataFrame lacks column: {r}, has {candles.columns}")


def create_label(row: pd.Series) -> str:
    """Create labels for a single candle."""


def make_candle_labels(
        df: pd.DataFrame,
        dollar_prices=True,
        base_token_name: Optional[str]=None,
        quote_token_name: Optional[str]=None,
        line_separator="<br>",
) -> pd.Series:
    """Generate individual labels for OHLCV chart candles.

    Used to display toolips on OHLCV chart.

    :poram candle_df:
        Candles for which we need tooltips.

    :poram label_df:
        A target dataframe

        A column "label" is generated and it is populated
        for every index timestamp that does not have label yet.

    :param dollar_prices:
        True if prices are in USD. Otherwise in the given quote token.

    :param quote_token_name:
        Cryptocurrency as the quote token pair.

    :param line_separator:
        New line format.

        Plotly wants raw HTML.

    :return:
        Series of text label
    """

    validate_ohclv_dataframe(df)

    if dollar_prices:
        if quote_token_name:
            price_text = f"{quote_token_name}/USD"
            volume_text = "USD"
        else:
            price_text = "USD"
            volume_text = "USD"
    else:
        assert quote_token_name, "Quote token must be given"
        price_text = f"{base_token_name} / {quote_token_name}"
        volume_text = quote_token_name

    # All label values are NA by default
    def _create_label_for_single_candle(row: pd.Series):
        # Index here can be MultiIndex as well, so assume timestamp is available as a column

        timestamp = row["timestamp"]

        percentage_change = (row.close - row.open) / row.open
        text = [
            f"{timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "",
            f"Open: {row.open} {price_text}",
            f"High: {row.high} {price_text}",
            f"Low: {row.low} {price_text}",
            f"Close: {row.close} {price_text}",
        ]

        if "volume" in row.index:
            text += [
                f"Volume: {row.volume} {volume_text}",
            ]

        text += [
            f"Change: {percentage_change:.2f} %",
            "",
        ]

        if "exchange_rate" in row.index:
            text += [f"Exchange rate: {row.exchange_rate} {quote_token_name} / USD", ""]

        if "buys" in row.index:
            text += [
                f"Buys: {row.buys} txs",
                f"Sells: {row.sells} txs",
                f"Total: {row.buys + row.sells} trades",
                ""
            ]

        return line_separator.join(text)

    return df.apply(_create_label_for_single_candle, axis="columns")


def visualise_ohlcv(
        candles: pd.DataFrame,
        chart_name: Optional[str] = None,
        y_axis_name: Optional[str] = "Price USD",
        volume_axis_name: Optional[str] = "Volume USD",
        height: int = 800,
        theme: str = "plotly_white",
        volume_bar_colour: str = "rgba(128,128,128,0.5)",
        volume_bar_mode = VolumeBarMode.overlay,
        labels: Optional[pd.Series] = None,
        num_detached_indicators: Optional[int] = 0,
        vertical_spacing: Optional[float] = 0.05,
        relative_sizing: Optional[list[float]]  = None,
        subplot_names: Optional[list[str]] = None,
        price_chart_rel_size: float = 1.0,
        subplot_rel_size: float = 0.2,
        subplot_font_size: int = 11,
) -> go.Figure:
    """Draw a candlestick chart.

    If the `candles` has `label` column this will be used
    as the mouse hover text for candles.

    If the `candles` has `volume` column, draw also this column.

    We remove the default "minimap" scrolling as it has pretty
    bad usability.

    :param chart_name:
        Will be displayed at the top of the chart

    :param y_axis_name:
        Will be displayed on an Y-axis

    :param height:
        Chart height in pixels

    :param theme:
        Plotly colour scheme for the chart.

        `See Plotly color scheme list here <https://plotly.com/python/templates/>`__.

    :param volume_bar_colour:
        Override the default colour for volume bars

    :param volume_bar_mode:

        Draw volume chart as a separate chart under the candlestick chart.

        If not set, draw as an overlay.

        `Note that Plotly does not allow reodering of tracing <https://github.com/plotly/plotly.py/issues/2345#issuecomment-809339043>`__,
        and the volume bars will always be on the top of the candlesticks.

    :param labels:
        Tooltip labels for candles.

        See :py:func:`make_candle_labels`
    
    :param num_detached_indicators:
        Number of indicators that will be drawn as separate charts.
    
    :param vertical_spacing:
        Vertical spacing between charts.
    
    :param relative_sizing:
        Sizing of subplots relative to the main price chart. 
        Price chart is regarded as 1.0, so subplots should be smaller than 1.0.
    
    :param subplot_names:
        Names of subplots. Used as titles for subplots
        
    :param price_chart_rel_size:
        Relative size of the price chart. Used as benchmark for relative sizing of subplots.
        Recommended is 1
    
    :param subplot_rel_size:
        Relative size of subplots. 
        
        If relative_sizing not provided, this will be used to set the same size for all subplots.
        
        If relative_sizing is provided, this will be used for volume subplot if it is not overlayed.

    :return:
        Plotly figure object
    """

    # sanity checks
    if num_detached_indicators > 0:
        if relative_sizing:
            assert len(relative_sizing) == num_detached_indicators, "relative sizing list must be the same length as num_detached_indicators"
        if subplot_names:
            assert len(subplot_names) == num_detached_indicators, "subplot_names list must be the same length as num_detached_indicators"
    
    validate_ohclv_dataframe(candles)

    if labels is not None:
        text = labels
    else:
        # TODO: Legacy - deprecate
        # Add change percentages on candle mouse hover
        percentage_changes = ((candles['close'] - candles['open'])/candles['open']) * 100
        text = ["Change: " + f"{percentage_changes[i]:.2f}%" for i in range(len(percentage_changes))]

    candlesticks = go.Candlestick(
        x=candles.index,
        open=candles['open'],
        high=candles['high'],
        low=candles['low'],
        close=candles['close'],
        showlegend=False,
        text=text,
        hoverinfo="text",
    )

    
    if "volume" in candles.columns:
        volume_bars = go.Bar(
                x=candles.index,
                y=candles['volume'],
                showlegend=False,
                marker={
                    "color": volume_bar_colour,
                }
            )
    else:
        volume_bars = None
    
    # Add volume to plot
    fig = _get_volume_grid(
        volume_bars, 
        volume_bar_mode, 
        volume_axis_name, 
        num_detached_indicators,
        vertical_spacing,
        relative_sizing,
        subplot_names,
        price_chart_rel_size,
        subplot_rel_size
    )
    
    # Set chart core options
    _set_chart_core_options(chart_name, y_axis_name, height, theme, fig)
    
    # Add candlesticks last since we want them on top if overlayed
    fig.add_trace(candlesticks, secondary_y=False)
    
    fig.update_annotations(font_size=subplot_font_size)

    return fig

def _set_chart_core_options(chart_name, y_axis_name, height, theme, fig):
    """Update figure layout. Set chart core options."""
    fig.update_layout(
        height=height,
        template=theme,
    )

    if chart_name:
        fig.update_layout(
            title=chart_name,
        )

    # Range slider is not very user friendly so just
    # disable it for now
    fig.update_xaxes(rangeslider={"visible": False})

    # Move legend to the bottom so we have more space for
    # time axis in narrow notebook views
    # https://plotly.com/python/legend/f
    fig.update_layout(
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1,
        })

def _get_volume_grid(
    volume_bars, 
    volume_bar_mode: bool, 
    volume_axis_name: str, 
    num_detached_indicators: int,
    vertical_spacing: float,
    relative_sizing: list[float],
    subplot_names: list[str],
    price_chart_rel_size: float,
    subplot_rel_size: float,
) -> go.Figure:
    """Get subplot grid, with volume information, based on the volume bar mode"""
    
    is_secondary_y = _get_secondary_y(volume_bar_mode)
    
    if relative_sizing and not all(relative_sizing):
        relative_sizing = None
    
    # blank subplot name for the main price chart
    if subplot_names:
        subplot_names.insert(0, None)

    if volume_bar_mode == VolumeBarMode.separate:
        # If separate, we need to use detached subplots
        
        if relative_sizing:
            row_heights = [price_chart_rel_size] + [subplot_rel_size] + relative_sizing
        else:
            row_heights = [subplot_rel_size for _ in range(num_detached_indicators+1)]
            row_heights.insert(0, price_chart_rel_size)

        # https://stackoverflow.com/a/65997291/315168
        # Add two rows for volume and price
        fig = make_subplots(
            rows=num_detached_indicators + 2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=vertical_spacing,
            row_heights=row_heights,
            row_titles=subplot_names,
        )

        if volume_bars is not None:
            # https://stackoverflow.com/a/65997291/315168
            _update_separate_volume(volume_bars, volume_axis_name, fig)

        return fig

    elif volume_bar_mode == VolumeBarMode.overlay:
        # If overlayed (or hidden), we need to use secondary Y axis
        # Add 1 row for price
        specs = [[{}] for _ in range(num_detached_indicators)]
        specs.insert(0, [{"secondary_y": is_secondary_y}])
        
        if relative_sizing:
            row_heights = [price_chart_rel_size] + relative_sizing
        else:
            row_heights = [subplot_rel_size for _ in range(num_detached_indicators)]
            row_heights.insert(0, price_chart_rel_size)
        
        fig = make_subplots(
            rows = num_detached_indicators + 1,
            cols = 1,
            specs=specs,
            shared_xaxes=True,
            row_heights=row_heights,
            vertical_spacing=vertical_spacing,
            row_titles=subplot_names,
        )

        if volume_bars is not None:
            # If overlayed, we need to add volume first
            _update_overlay_volume(volume_bars, volume_axis_name, fig)

        return fig

    elif volume_bar_mode == VolumeBarMode.hidden:
        specs = [[{}] for _ in range(num_detached_indicators)]
        specs.insert(0, [{"secondary_y": is_secondary_y}])
        
        if relative_sizing:
            row_heights = [price_chart_rel_size] + relative_sizing
        else:
            row_heights = [subplot_rel_size for _ in range(num_detached_indicators)]
            row_heights.insert(0, price_chart_rel_size)

        # No volume
        return make_subplots(
            rows = num_detached_indicators + 1,
            cols = 1,
            specs=specs,
            shared_xaxes=True,
            vertical_spacing=vertical_spacing,
            row_heights=row_heights,
            row_titles=subplot_names,
        )

    else:
        raise ValueError(f"Unknown volume bar mode: {volume_bar_mode}")

def _update_overlay_volume(volume_bars, volume_axis_name, fig):
    """Update overlay volume chart info"""
    fig.add_trace(volume_bars, secondary_y=True)
    fig.update_yaxes(secondary_y=True, showgrid=False)

    if volume_axis_name:
        fig.update_yaxes(title=volume_axis_name, secondary_y=True, row=1)

def _update_separate_volume(volume_bars, volume_axis_name, fig):
    """Update detached volume chart info"""
    fig.add_trace(volume_bars, row=2, col=1)

    # volume axis name added to subplot title for now (right side instead of left)
    # if volume_axis_name:
    #    fig.update_yaxes(title=volume_axis_name, row=2)

def _get_secondary_y(volume_mode: VolumeBarMode) -> bool:
    """Based on the volume bar mode, should we use secondary Y axis?
    
    Secondary data may not have volume available"""
    if volume_mode == VolumeBarMode.overlay:
        return True
    elif volume_mode in [VolumeBarMode.hidden, VolumeBarMode.separate]:
        return False
    else:
        raise ValueError(f"Unknown volume bar mode: {volume_mode}")
            
