"""Price (OHLCV) charting utilities.

Draw price charts using Plotly.

- Candle charts

- Open, high, low, close (OHLC)

- Open, high, low, close, volume (OHLCV)

- Dark and light themes
"""
import logging
from typing import Optional

import plotly.graph_objects as go
import pandas as pd
from backtrader import List
from plotly.subplots import make_subplots


logger = logging.getLogger(__name__)



class BadOHLCVData(Exception):
    """We could not figure out the data frame"""


def validate_ohclv_dataframe(candles: pd.DataFrame):
    required_columns = ["timestamp", "open", "close", "high", "low"]
    for r in required_columns:
        if r not in candles.columns:
            raise BadOHLCVData(f"OHLCV DataFrame lacks column: {r}, has {candles.columns}")



def make_candle_labels(
        df: pd.DataFrame,
        dollar_prices=True,
        base_token_name: Optional[str]=None,
        quote_token_name: Optional[str]=None) -> int:
    """Generate individual labels for OHLCV chart candles.

    This is a regenerative function: you can
    call it when new data has been appended to the
    DataFrame and it won't recalculate already existing labels.

    :poram df:
        OHLCV dataframe which is modified in place.

        A column "label" is generated and it is populated
        for every index timestamp that does not have label yet.

    :param dollar_prices:
        True if prices are in USD. Otherwise in the given quote token.

    :param quote_token_name:
        Cryptocurrency as the quote token pair.

    :return:
        Number of labels generated
    """

    validate_ohclv_dataframe(df)

    labels = []

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

    # Create label in-place so we retain the same index.
    # All labels have NaN as their initial value.
    if "label" not in df.columns:
        df["label"] = pd.Series(dtype=str)

    index = []
    data = []

    # All label values are NA by default
    for idx, row in df.loc[df.label.isna()].iterrows():
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

        if "volume" in df.columns:
            text += [
                f"Volume: {row.volume} {volume_text}",
            ]

        text += [
            f"Change: {percentage_change:.2f} %",
            "",
        ]

        if "exchange_rate" in df.columns:
            labels += [f"Exchange rate: {row.exchange_rate}", ""]

        if "buys" in df.columns:
            text += [
                f"Buys: {row.buys} txs",
                f"Sells: {row.sells} txs",
                f"Total: {row.buys + row.sells} trades",
                ""
            ]

        index.append(idx)
        data.append("\n".join(text))

    new_labels = pd.DataFrame({"label": data}, index=index, dtype=str)

    df.update(new_labels)

    return len(data)


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

    :return:
        Plotly figure object
    """

    validate_ohclv_dataframe(candles)

    if "label" in candles.columns:
        text = candles["label"]
        logger.info("Drawing with labels")
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
