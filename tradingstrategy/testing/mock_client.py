"""Mock client implementations that reads data from JSON files.

Used for unit testing

- Create on-chain universe in a test blockchain backend like Anvil

- Export this data for the mock client

"""

from pyarrow import Table

from tradingstrategy.client import BaseClient
from tradingstrategy.timebucket import TimeBucket


class MockClientNotImplemented(NotImplementedError):
    """Mark the exceptions so that the consumer can catch them and knows they are dealing with a mock client."""


class MockClient(BaseClient):
    """Have all methods marked as not implemented"""

    def fetch_exchange_universe(self):
        raise MockClientNotImplemented()

    def fetch_pair_universe(self):
        raise MockClientNotImplemented()

    def fetch_all_candles(self):
        raise MockClientNotImplemented()

    def fetch_candles_by_pair_ids(self):
        raise MockClientNotImplemented()

    def fetch_trading_data_availability(self):
        raise MockClientNotImplemented()

    def fetch_candle_dataset(self, bucket: TimeBucket):
        raise MockClientNotImplemented()

    def fetch_all_liquidity_samples(self, bucket: TimeBucket) -> Table:
        raise MockClientNotImplemented()



