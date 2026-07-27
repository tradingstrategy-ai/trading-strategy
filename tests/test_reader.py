from pathlib import Path

import pytest

from tradingstrategy import reader
from tradingstrategy.reader import BrokenData, read_parquet


def test_read_parquet_wraps_os_error_with_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Parquet ``OSError`` keeps the file path for cache retry cleanup.

    1. Mock PyArrow to raise the same error class seen in CI.
    2. Read a temporary parquet path through ``read_parquet()``.
    3. Assert the raised ``BrokenData`` carries the original file path.
    """
    parquet_path = tmp_path / "broken.parquet"

    def mock_read_table(*args, **kwargs):
        raise OSError("Column cannot have more than one dictionary.")

    # 1. Mock PyArrow to raise the same error class seen in CI.
    monkeypatch.setattr(reader.pq, "read_table", mock_read_table)

    # 2. Read a temporary parquet path through ``read_parquet()``.
    with pytest.raises(BrokenData) as exc_info:
        read_parquet(parquet_path)

    # 3. Assert the raised ``BrokenData`` carries the original file path.
    assert exc_info.value.path == parquet_path
