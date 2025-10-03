import logging
import os
from contextlib import contextmanager
from pathlib import Path

from filelock import FileLock

logger = logging.getLogger(__name__)


@contextmanager
def wait_other_writers(path: Path | str, timeout: int = 120):
    """Wait other potential writers writing the same file.

    - Work around issues when parallel unit tests and such
      try to write the same file

    Example:

    .. code-block:: python

        import urllib
        import tempfile

        import pytest
        import pandas as pd

        @pytest.fixture()
        def my_cached_test_data_frame() -> pd.DataFrame:

            # Al tests use a cached dataset stored in the /tmp directory
            path = os.path.join(tempfile.gettempdir(), "my_shared_data.parquet")

            with wait_other_writers(path):

                # Read result from the previous writer
                if not path.exists(path):
                    # Download and write to cache
                    urllib.request.urlretrieve("https://example.com", path)

                return pd.read_parquet(path)

    :param path:
        File that is being written

    :param timeout:
        How many seconds wait to acquire the lock file.

        Default 2 minutes.

    :raise filelock.Timeout:
        If the file writer is stuck with the lock.
    """

    if isinstance(path, str):
        path = Path(path)

    assert isinstance(path, Path), f"Not Path object: {path}"

    assert path.is_absolute(), f"Did not get an absolute path: {path}\n" \
                               f"Please use absolute paths for lock files to prevent polluting the local working directory."

    # If we are writing to a new temp folder, create any parent paths
    os.makedirs(path.parent, exist_ok=True)

    # https://stackoverflow.com/a/60281933/315168
    lock_file = path.parent / (path.name + '.lock')

    lock = FileLock(lock_file, timeout=timeout)

    if lock.is_locked:
        logger.info(
            "Parquet file %s locked for writing, waiting %f seconds",
            path,
            timeout,
        )

    with lock:
        yield
