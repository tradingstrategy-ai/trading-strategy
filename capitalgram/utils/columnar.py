"""Columnar data manipulation utilities."""

from typing import Iterable, Dict


def iterate_columnar_dicts(inp: Dict[str, list]) -> Iterable[Dict[str, object]]:
    """Iterates columnar dict data as rows.

    Useful for constructing rows/objects out from :py:class:`pyarrow.Table` or :py:class:`pyarrow.RecordBatch`.

    :param inp: Input dictionary of lists e.g. one from :py:method:`pyarrow.RecordBatch.to_pydict`. All lists in the input must be equal length.

    :return: Iterable that gives one dictionary per row after transpose
    """
    keys = inp.keys()
    for i in range(len(inp)):
        item = {key: inp[key][i] for key in keys}
        yield item
