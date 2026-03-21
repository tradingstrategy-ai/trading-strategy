"""Helpers to switch Pandas to Arrow-backed dtypes where supported."""

import functools
import logging

import pandas as pd

logger = logging.getLogger(__name__)

_ARROW_DTYPE_READERS = (
    "read_csv",
    "read_json",
    "read_parquet",
)

_configured = False


def _wrap_reader_with_arrow_backend(reader):
    """Force Arrow dtype backend for Pandas IO helpers that support it."""

    @functools.wraps(reader)
    def wrapped(*args, **kwargs):
        kwargs.setdefault("dtype_backend", "pyarrow")
        return reader(*args, **kwargs)

    return wrapped


def configure_pandas_arrow_backend() -> None:
    """Enable Arrow-backed string and IO dtypes for this process.

    .. note::

        The IO reader wrapping (forcing ``dtype_backend="pyarrow"`` on
        ``read_parquet`` etc.) is disabled because it produces
        ``ArrowDtype("timestamp[ns]")`` indices that fail
        ``isinstance(index, pd.DatetimeIndex)`` checks throughout
        the codebase.  Re-enable once all downstream assertions and
        groupby/resample operations are Arrow-aware.
    """
    global _configured

    if _configured:
        return

    pd.options.mode.string_storage = "pyarrow"
    # Do NOT set pd.options.future.infer_string = True or wrap IO readers
    # until the codebase is fully compatible with ArrowDtype indices.

    _configured = True
    logger.info("Pandas Arrow string storage enabled (IO reader wrapping disabled)")
