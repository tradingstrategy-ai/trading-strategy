"""Reading and consuming datasets."""

import io

import pyarrow as pa
from pyarrow import parquet as pq


def read_parquet(stream: io.BytesIO) -> pa.Table:
    """Reads compressed Parquet file of data to memory.

    File or stream can describe :py:class:`capitalgram.candle.Candle` or :py:class:`capitalgram.pair.DEXPair` data.

    :param stream: A file input that must support seeking.
    """
    # https://arrow.apache.org/docs/python/parquet.html
    table = pq.read_table(stream)
    return table