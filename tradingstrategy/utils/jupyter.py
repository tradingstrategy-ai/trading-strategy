"""Helpers to deal with Jupyter Notebook issues."""
import enum
from typing import Callable

import pandas as pd
from IPython import get_ipython
from IPython.display import display
from IPython.terminal.interactiveshell import TerminalInteractiveShell
from ipykernel.zmqshell import ZMQInteractiveShell


class JupyterOutputMode(enum.Enum):
    """What kind of output Jupyter Notebook supports."""

    #: We could not figure out - please debug
    unknown = "unknown"

    #: The notebook is run by terminal
    terminal = "terminal"

    #: Your normal HTML notebook
    html = "html"


def get_notebook_output_mode() -> JupyterOutputMode:
    """Determine if the Jupyter Notebook supports HTML output."""

    # See https://stackoverflow.com/questions/70768390/detecting-if-ipython-notebook-is-outputting-to-a-terminal
    # for discussion
    ipython = get_ipython()

    if isinstance(ipython, TerminalInteractiveShell):
        # Hello ANSI graphics my old friend
        return JupyterOutputMode.terminal
    elif isinstance(ipython, ZMQInteractiveShell):
        # MAke an assumption ZMQ instance is a HTML notebook
        return JupyterOutputMode.html

    return JupyterOutputMode.unknown


def display_with_styles(df: pd.DataFrame, apply_styles_func: Callable):
    """Display a Pandas dataframe as a table.

    DataFrame styler objects only support HTML output.
    If the Jupyter Notebook output does not have HTML support,
    (it is a command line), then display DataFrame as is
    without styles.

    For `apply_style_func` example see :py:method:`tradingstrategy.analysis.portfolioanalyzer.expand_timeline`.

    :param df: Pandas Dataframe we want to display as a table.

    :param apply_styles_func: A function to call on DataFrame to add its styles on it.
        We need to pass this as callable due to Pandas architectural limitations.
        The function will create styles using `pandas.DataFrame.style` object.
        However if styles are applied the resulting object can no longer be displayed in a terminal.
        Thus, we need to separate the procses of creating dataframe and creating styles and applying them.

    """
    mode = get_notebook_output_mode()
    if mode == JupyterOutputMode.html:
        display(apply_styles_func(df))
    else:
        display(df)
