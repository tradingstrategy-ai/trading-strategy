"""Reading and consuming datasets."""

import io
import logging
from pathlib import Path

import pyarrow as pa
from pyarrow import parquet as pq, ArrowInvalid

logger = logging.getLogger(__name__)


class BrokenData(Exception):
    """Raised when we cannot read Parquet file for some reason."""

    def __init__(self, msg: str, path: Path):
        super().__init__(msg)
        self.path = path


def read_parquet(path: Path) -> pa.Table:
    """Reads compressed Parquet file of data to memory.

    File or stream can describe :py:class:`tradingstrategy.candle.Candle` or :py:class:`tradingstrategy.pair.DEXPair` data.

    :param stream: A file input that must support seeking.
    """
    assert isinstance(path, Path), f"Expected path: {path}"
    f = path.as_posix()
    logger.info("Reading Parquet %s", f)
    # https://arrow.apache.org/docs/python/parquet.html
    try:
        table = pq.read_table(f)
    except ArrowInvalid as e:
        raise BrokenData(f"Could not read Parquet file: {f}\n"
                         f"Probably a corrupted download.\n"
                         f"See https://tradingstrategy.ai/docs/programming/troubleshooting.html#resetting-the-download-cache\n"
                         f"for instructions how to clear download cache and remove corrupted files, or try the command line:\n"
                         f"rm '{f}'",
                         path=path) \
                        from e
    return table
