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
    required_columns = ["timestamp", "open", "close", "high", "low"]
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

    :return:
        Plotly figure object
    """

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

    volume_available = "volume" in candles.columns

    if volume_bar_mode != VolumeBarMode.hidden:
        # Synthetic data may not have volume available
        if volume_bar_mode == VolumeBarMode.overlay:
            should_create_volume_subplot = True
        else:
            should_create_volume_subplot = False
    else:
        should_create_volume_subplot = False

    # We need to use sublot to make volume bar overlay
    if volume_bar_mode == VolumeBarMode.separate:
        # https://stackoverflow.com/a/65997291/315168
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            row_width=[0.2, 0.7])
    else:
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

    # Do we need volume overlay?
    if volume_available:
        volume_bars = go.Bar(
            x=candles.index,
            y=candles['volume'],
            showlegend=False,
            marker={
                "color": volume_bar_colour,
            }
        )

        if volume_bar_mode == VolumeBarMode.overlay:
            fig.add_trace(volume_bars, secondary_y=True)
            fig.update_yaxes(secondary_y=True, showgrid=False)

            if volume_axis_name:
                fig.update_yaxes(title=volume_axis_name, secondary_y=True, row=1)
    else:
        volume_bars = None

    fig.add_trace(candlesticks, secondary_y=False)

    # Add the separate volume chart below
    if volume_available:
        if volume_bar_mode == VolumeBarMode.separate:
            # https://stackoverflow.com/a/65997291/315168
            fig.add_trace(volume_bars, row=2, col=1)

            if volume_axis_name:
                fig.update_yaxes(title=volume_axis_name, row=2)

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
