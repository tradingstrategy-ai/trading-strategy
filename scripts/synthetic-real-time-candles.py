"""An example script how to visualise real-time candles.

- The OHLCV data is randomly generated

- You need to install `dash` to run this application

.. code-block:: shell

    pip install dash

- Then run the application:
"""
from typing import Tuple

import pandas as pd
import dash
from dash import html, dcc, Output, Input

from eth_defi.price_oracle.oracle import TrustedStablecoinOracle, FixedPriceOracle
from tradingstrategy.direct_feed.candle_feed import CandleFeed
from tradingstrategy.direct_feed.reorg_mon import SyntheticReorganisationMonitor
from tradingstrategy.direct_feed.synthetic_feed import SyntheticTradeFeed
from tradingstrategy.direct_feed.timeframe import Timeframe


def setup_candle_generator() -> Tuple[SyntheticReorganisationMonitor, CandleFeed, SyntheticTradeFeed]:
    """Create the synthetic blockchain and trading pairs.

    This will generate random candle data to display.
    """

    # Setup a mock blockchain with 3 seconds block time
    block_duration_seconds = 1.5
    mock_chain = SyntheticReorganisationMonitor(block_duration_seconds=block_duration_seconds)

    # Generate 5 min candles
    timeframe = Timeframe("5min")

    # Start with 1 hour data
    mock_chain.produce_blocks(3600 / block_duration_seconds)

    pairs = ["ETH-USD", "AAVE-ETH"]

    # Have two pairs
    trade_feed = SyntheticTradeFeed(
        pairs,
        {
            "ETH-USD": TrustedStablecoinOracle(),
            "AAVE-ETH": FixedPriceOracle(1600),
        },
        mock_chain,
        timeframe=timeframe,
        min_amount=-50,
        max_amount=50,
        prices = {
            "ETH-USD": 1600,
            "AAVE-ETH": 0.1,
        }
    )

    candle_feed = CandleFeed(
        pairs,
        timeframe=timeframe,
    )
    return mock_chain, candle_feed, trade_feed


# https://community.plotly.com/t/please-help-with-plot-callback-function-error/46168
df_btc = pd.read_csv("data/livedata.csv")

app = dash.Dash(__name__)
app.layout = html.Div(
    [
        html.Div([
            dcc.Graph(id='live_graph', animate=True, style={"height": "100vh"}),
            dcc.Interval(
                id='interval_component',
                interval=2000,
            ),
        ]),
    ],
    style = {"height": "100vh"}
)

@app.callback(Output('live_graph', 'figure'),
        [Input('interval_component', 'n_intervals')])
def graph_update(n):
    df_btc = pd.read_csv("data/livedata.csv")
    graph_candlestick = go.Candlestick(x=list(btc_date),
                            open=list(btc_open),
                            high=list(btc_high),
                            low=list(btc_low),
                            close=list(btc_close),
                            xaxis="x",
                            yaxis="y",
                            visible=True)

    graph_rsi = get_rsi(df_btc)
    return {'data': [graph_rsi, graph_candlestick], 'layout': go.Layout(xaxis=dict(range=[min(btc_date),max(btc_date)]),
                                                yaxis=dict(range=[min(btc_low),max(btc_high)],),
                                                yaxis2=dict(range=[0,100], overlaying='y', side='right'),) }


if __name__ == '__main__':
    app.run_server(debug=True)