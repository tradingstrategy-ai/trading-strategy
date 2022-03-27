"""Reading and consuming datasets."""

import io
import logging
from pathlib import Path

import pyarrow as pa
from pyarrow import parquet as pq


logger = logging.getLogger(__name__)


def read_parquet(path: Path) -> pa.Table:
    """Reads compressed Parquet file of data to memory.

    File or stream can describe :py:class:`tradingstrategy.candle.Candle` or :py:class:`tradingstrategy.pair.DEXPair` data.

    :param stream: A file input that must support seeking.
    """
    assert isinstance(path, Path), f"Expected path: {path}"
    f = path.as_posix()
    logger.info("Reading Parquet %s", f)
    # https://arrow.apache.org/docs/python/parquet.html
    table = pq.read_table(f)
    return table
