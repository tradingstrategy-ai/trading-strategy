"""Storing trade feed data on disk."""
import abc
import logging
import os
import shutil
import tempfile
from pathlib import Path

import pandas as pd


logger = logging.getLogger(__name__)


class TradeFeedStore(abc.ABC):
    """Describe trade feed storage backend."""

    @abc.abstractmethod
    def is_pristine(self) -> bool:
        """State has not been written yet."""

    @abc.abstractmethod
    def load(self) -> pd.DataFrame:
        """Load the state from the storage."""

    @abc.abstractmethod
    def save(self, df: pd.DataFrame):
        """Save the state to the storage."""

    def create(self) -> pd.DataFrame:
        """Create a new state storage.

        :param name:
            Name of the strategy this State belongs to
        """
        return pd.DataFrame()


class ParquetFileStore(TradeFeedStore):
    """Store trades on a Parquet file on a file system."""

    def __init__(self, path: Path):
        self.path = path

    def is_pristine(self) -> bool:
        return not self.path.exists()

    def load(self) -> pd.DataFrame:
        logger.info("Loading trades from %s", self.path)
        with open(self.path, "rt") as inp:
            return pd.read_parquet(inp)

    def save(self, df: pd.DataFrame):
        """Write a new Parquet dump using Linux atomic file replacement."""
        dirname, basename = os.path.split(self.path)
        temp = tempfile.NamedTemporaryFile(mode='wt', delete=False, dir=dirname)
        with open(temp.name, "wt") as out:
            df.to_parquet(out)
        temp.close()
        shutil.move(temp.name, self.path)

    def create(self) -> pd.DataFrame:
        logger.info("Created new trade data store for %s", self.path)
        return super().create()
