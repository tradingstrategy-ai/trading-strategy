"""Price (OHLCV) charting utilities.

Draw price charts using Plotly.

- Candle charts

- Open, high, low, close (OHLC)

- Open, high, low, close, volume (OHLCV)

- Dark and light themes
"""
from typing import Optional

import plotly.graph_objects as go
import pandas as pd
from plotly.subplots import make_subplots



class BadOHLCVData(Exception):
    """We could not figure out the data frame"""


def validate_ohclv_dataframe(candles: pd.DataFrame):
    required_columns = ["open", "close", "high", "low"]
    for r in required_columns:
        if r not in candles.columns:
            raise BadOHLCVData(f"OHLCV DataFrame lacks column: {r}, has {candles.columns}")



def visualise_ohlcv(
        candles: pd.DataFrame,
        chart_name: Optional[str] = None,
        y_axis_name: Optional[str] = "Price USD",
        volume_axis_name: Optional[str] = "Volume USD",
        height: int = 800,
        theme: str = "plotly_white",
        volume_bar_colour: str = "rgba(128,128,128,0.5)",
) -> go.Figure:
    """Draw a candlestick chart.

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

    :return:
        Plotly figure object
    """

    validate_ohclv_dataframe(candles)

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
    )

    # Synthetic data may not have volume available
    should_create_volume_subplot: bool = "volume" in candles.columns

    # We need to use sublot to make volume bars
    fig = make_subplots(specs=[[{"secondary_y": should_create_volume_subplot}]])

    # Set chart core options
    fig.update_layout(
        height=height,
        template=theme,
    )

    if chart_name:
        fig.update_layout(
            title=chart_name,
        )

    fig.update_yaxes(secondary_y=False, showgrid=True)
    if y_axis_name:
        fig.update_yaxes(title=y_axis_name)

    # Range slider is not very user friendly so just
    # disable it for now
    fig.update_xaxes(rangeslider={"visible": False})

    if should_create_volume_subplot:
        volume_bars = go.Bar(
            x=candles.index,
            y=candles['volume'],
            showlegend=False,
            marker={
                "color": volume_bar_colour,
            }
        )
        fig.add_trace(volume_bars, secondary_y=True)

        fig.update_yaxes(secondary_y=True, showgrid=False)

        if volume_axis_name:
            fig.update_yaxes(title=volume_axis_name, secondary_y=True)

    fig.add_trace(candlesticks, secondary_y=False)

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

    return fig
