"""Show histograms how many trades were profitable or not."""

import numpy as np
import pandas as pd
import matplotlib.pyplot as pyplot
from matplotlib.axes import SubplotBase
from matplotlib.axes._base import _AxesBase
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.container import BarContainer
from matplotlib.figure import Figure



def plot_trade_profit_distribution(df: pd.DataFrame, bins=25, cap_profit=1.1, vmin=-1, vmax=0.5) -> Figure:
    """Create a histogram of won and lost trades based on trade analyzer expanded timeline output.

    See also :py:meth:`tradingstrategy.analysis.tradeanalyizer.expand_timeline`.

    :param df: A DataFrame with a column `PnL % raw`

    :param cap_profit: If profit is higher than this, cap the value so that one off mega profits to not break the diagram
    """

    colormap: LinearSegmentedColormap = pyplot.cm.get_cmap("RdYlGn")
    df = df[["PnL % raw"]].clip(-1, cap_profit)

    # A single successfuk trade might be something like 400% or 4.0
    max_profit_pct = df["PnL % raw"].max()
    min_profit_pct = df["PnL % raw"].min()

    # https://stackoverflow.com/questions/23061657/plot-histogram-with-colors-taken-from-colormap
    Y: np.array
    X: np.array
    # https://numpy.org/doc/stable/reference/generated/numpy.histogram.html
    # Always adjust left X to -100% or trade lost all money
    Y, X = np.histogram(df, bins, range=(min_profit_pct, max_profit_pct))
    x_span = X.max() - X.min()

    C = []
    for x in X:

        # Map x back to the range -100% to 100% - 100% is the max green.
        # Do not directly use the x co-ordinate on the bar diagram for the color
        # as middle is not zero

        # CLip
        profit_clipped = min(vmax, x)
        profit_clipped = max(vmin, profit_clipped)
        green_percent = profit_clipped / (vmax - vmin)
        colormap_adjusted = green_percent/2 + 0.5  # 0 ... 1.0

        c = colormap(colormap_adjusted)
        # print(c, x, profit_pct, X.min(), x_span)
        C.append(c)

    fig: Figure
    ax: _AxesBase
    fig, ax = pyplot.subplots()

    # https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.bar.html
    bar_container: BarContainer = ax.bar(
        x=X[:-1],
        height=Y,
        color=C,
        width=X[1] - X[0])

    xtick_labels = [f"{x:.0%}" for x in ax.get_xticks()]
    ax.set_xlabel("Trade profit %")
    ax.set_ylabel("Number of trades")
    ax.set_xticklabels(xtick_labels)
    ax.set_title("Trade won/lost distribution")

    # Retina!
    fig.set_dpi(600)

    return fig

