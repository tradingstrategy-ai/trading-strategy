"""Helpers to deal with Jupyter Notebook issues."""
import enum
import sys
from typing import Callable, Collection

import pandas as pd
from IPython import get_ipython
from IPython.display import display
from IPython.terminal.interactiveshell import TerminalInteractiveShell

try:

    # E DeprecationWarning: Jupyter is migrating its paths to use standard platformdirs
    # E   given by the platformdirs library.  To remove this warning and
    # E   see the appropriate new directories, set the environment variable
    # E  `JUPYTER_PLATFORM_DIRS=1` and then run `jupyter --paths`.
    # E   The use of platformdirs will be the default in `jupyter_core` v6
    import os
    if "JUPYTER_PLATFORM_DIRS" not in os.environ:
        os.environ["JUPYTER_PLATFORM_DIRS"] = "true"

    from ipykernel.zmqshell import ZMQInteractiveShell
    HAS_JUPYTER_EVENT_LOOP = True
except ImportError:
    HAS_JUPYTER_EVENT_LOOP = False


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

    assert HAS_JUPYTER_EVENT_LOOP, "Did not detect Jupyter during import time"

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


def is_pyodide() -> bool:
    """Are we running under Pyodide / JupyterLite notebook.

    `See Pyodide documentation <https://pyodide.org/en/stable/usage/faq.html#how-to-detect-that-code-is-run-with-pyodide>`__.
    """
    return sys.platform == 'emscripten'


def make_clickable(text, url):
    """Format a value as a clickable link.
    
    :param text:
        Text to display for the link.

    :param url:
        URL to link to.

    :return:
        HTML string with a clickable link.
    """
    return '<a href="{}">{}</a>'.format(url, text)


def format_links_for_html_output(df: pd.DataFrame, link_columns: Collection[str]) -> pd.DataFrame:
    """Pandas DataFrame helper to format column link values as clicable.

    - Normal links are too long to display

    - Only applicable to HTML output, in a not terminal

    :param link_columns:
        Columns where the value is a URL

    :return:
        New DataFrame where URLs have been converted to links with "View" label

    """

    if get_notebook_output_mode() == JupyterOutputMode.html:
        for c in link_columns:
            df[c] = df[c].apply(lambda url: make_clickable("View", url))
    else:
        df = df.assign(**{c: "<in console>" for c in link_columns})

    return df
