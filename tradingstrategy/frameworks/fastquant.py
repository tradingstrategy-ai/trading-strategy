"""The :term:`Fastquant` integration for Capitalgram data and notebooks."""

import pandas as pd


#: What column we care about in :py:meth`fastquant.backtest` results
#: and what they are in a human language
INTERESTING_COLUMNS = {
    "sharperatio": "Sharpe ratio",
    "maxdrawdown": "Maximum drawdown (%)",
    "pnl": "Profit and loss (USD)",
    "rtot": "Total returns (x)",
    "win_rate": "Win rate (%)",
    "won": "Won trades (n)",
    "lost": "Lost trades (n)",
}

FORMATTERS = {
    "(%)": "{:.2f} %",
    "(n)": "{:,}",
    "(USD)": "{:,.2f}",
}


def to_human_readable_result(df: pd.DataFrame) -> pd.DataFrame:
    """Convert the result dataframe object from Fastquant backtest() to something more human-readable.

    See :py:meth:`fastquant.backtest`.

    :return: A Dataframe objec than can be directly fed to :py:meth:`IPython.display.display`.
    """

    # Take a copy of keys we are only interested in

    columns_we_want = list(INTERESTING_COLUMNS.keys())

    df: pd.DataFrame = df[[*columns_we_want]]

    # Fix by hand
    df["win_rate"][0] = df["win_rate"][0] * 100

    df = df.rename(columns=INTERESTING_COLUMNS)

    # https://stackoverflow.com/a/55624414/315168
    for column in INTERESTING_COLUMNS.values():
        for formatter in FORMATTERS.keys():
            if formatter in column:
                fstring = FORMATTERS[formatter]
                df[column][0] = fstring.format(df[column][0])
                break

    # Switch to headers on columns mode
    transposed = df.transpose()

    transposed.style.hide_index()

    return transposed

