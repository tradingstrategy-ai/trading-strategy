"""Reading and consuming datasets."""

import logging
from pathlib import Path
from typing import Optional, List, Tuple

import pandas as pd
from fastparquet import ParquetFile

logger = logging.getLogger(__name__)


class BrokenData(Exception):
    """Raised when we cannot read Parquet file for some reason."""

    def __init__(self, msg: str, path: Path):
        super().__init__(msg)
        self.path = path


def read_parquet_pyarrow(path: Path, filters: Optional[List[Tuple]]=None) -> "pyarrow.Table":
    """Reads compressed Parquet file of data to memory.

    File or stream can describe :py:class:`tradingstrategy.candle.Candle`
    or :py:class:`tradingstrategy.pair.DEXPair` data.

    Filtering of candle data can be done
    during the read time, so that large dataset files do not need
    to be fully loaded to the memory. This severely reduces
    the RAM usage for low-memory environments when dealing Parquet.

    .. note ::

        This function has been deprecated in the favour of :py:funct:`read_parquet_fastparquet`.

    `For filtering options see Parquet documentation <https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html>`_.

    See :py:func:`pyarrow.parquet.read_table`.

    :param stream:
        A file input that must support seeking.

    :param filters:
        Parquet read_table filters.

    """

    import pyarrow as pa
    from pyarrow import parquet as pq, ArrowInvalid

    assert isinstance(path, Path), f"Expected path: {path}"
    f = path.as_posix()
    logger.info("Reading Parquet %s", f)
    # https://arrow.apache.org/docs/python/parquet.html
    try:
        table = pq.read_table(f, filters=filters)
    except ArrowInvalid as e:
        raise BrokenData(f"Could not read Parquet file: {f}\n"
                         f"Probably a corrupted download.\n"
                         f"See https://tradingstrategy.ai/docs/programming/troubleshooting.html#resetting-the-download-cache\n"
                         f"for instructions how to clear download cache and remove corrupted files, or try the command line:\n"
                         f"rm '{f}'",
                         path=path) \
                        from e
    return table


def read_parquet_fastparquet(path: Path) -> "pd.DataFrame":
    """Read Parquent input file using fastparquet library."

    - `More about fastparquet <https://pypi.org/project/fastparquet/>`__

    .. note ::

        We modify the returned Python object to have no-op method
        `to_pandas()` to make sure we do not break legacy code that
        was using :py:class:`pyarrow.Table` right away.

    :param path:
        Local file system path

    :return:
        The read file as Pandas DataFrame
    """

    pf = ParquetFile(path)

    # Deal with row groups
    dfs = []
    for df in pf.iter_row_groups():
        dfs.append(df)

    df = pd.concat(dfs)

    df.to_pandas = lambda: df

    return df
