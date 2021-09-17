import datetime
import io
from abc import ABC, abstractmethod
from typing import List

from tradingstrategy.caip import ChainAddressTuple
from tradingstrategy.timebucket import TimeBucket


class BaseTransport(ABC):
    """Define transport interface.

    Different transports can be used to get the candle data from oracle, depending on
    the execution context of the Python code.
    """

    @abstractmethod
    def fetch_stats(self) -> dict:
        pass

    @abstractmethod
    def fetch_pair_universe(self) -> io.BytesIO:
        """Get the latest info on trading pairs.

        :return: A reader for JSON and ZSTD serialised PairUniverse
        """

    @abstractmethod
    def fetch_live_candles(self, pair_list: List[ChainAddressTuple], start: datetime.datetime, end: datetime.datetime):
        """Downlaod real-time partial candle data."""

    @abstractmethod
    def fetch_candle_dataset(self, bucket: TimeBucket):
        """Download cached precompiled data set.

        Datasets are anywhere between 80 MB - 4 GB.
        """