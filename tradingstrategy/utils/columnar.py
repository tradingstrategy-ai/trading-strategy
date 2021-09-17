"""Columnar data manipulation utilities."""

from typing import Iterable, Dict


def iterate_columnar_dicts(inp: Dict[str, list]) -> Iterable[Dict[str, object]]:
    """Iterates columnar dict data as rows.

    Useful for constructing rows/objects out from :py:class:`pyarrow.Table` or :py:class:`pyarrow.RecordBatch`.

    Example:

    .. code-block:: python

        @classmethod
        def create_from_pyarrow_table(cls, table: pa.Table) -> "PairUniverse":
            pairs = {}
            for batch in table.to_batches(max_chunksize=5000):
                d = batch.to_pydict()
                for row in iterate_columnar_dicts(d):
                    pairs[row["pair_id"]] = DEXPair.from_dict(row)

            return PairUniverse(pairs=pairs)

    :param inp: Input dictionary of lists e.g. one from :py:method:`pyarrow.RecordBatch.to_pydict`. All lists in the input must be equal length.

    :return: Iterable that gives one dictionary per row after transpose
    """
    keys = inp.keys()
    first_item = next(iter(inp.values()))
    data_len = len(first_item)
    for i in range(data_len):
        item = {key: inp[key][i] for key in keys}
        yield item
