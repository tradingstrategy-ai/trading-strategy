"""Mock client implementations that reads data from JSON files.

Used for unit testing

- Create on-chain universe in a test blockchain backend like Anvil

- Export this data for the mock client

"""

from pyarrow import Table

from tradingstrategy.client import BaseClient
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.exchange import ExchangeUniverse
from tradingstrategy.pair import PandasPairUniverse


class MockClientNotImplemented(NotImplementedError):
    """Mark the exceptions so that the consumer can catch them and knows they are dealing with a mock client."""


class MockClient(BaseClient):
    """Have all methods marked as not implemented"""

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
    
    def get_default_quote_token_address(self, factory_address: str | None = None) -> str:
        """Get the quote token address used in the generated pair map.

        Helper method for setting up simple local dev routing.

        Returns a the first quote token address found in the pair universe.

        :param factory_address:
            The factory address to get the quote token address from.
        """
        quote_tokens = []
        pairs_df = self.fetch_pair_universe().to_pandas()
        pair_universe = PandasPairUniverse(pairs_df)
        assert pair_universe.get_count() > 0, "Pair universe has no trading pairs"
        for pair in pair_universe.iterate_pairs():
            if factory_address:
                if pair.exchange_address.lower() == factory_address.lower():
                    quote_tokens.append(pair.quote_token_address)
            else:
                quote_tokens.append(pair.quote_token_address)
        #assert len(quote_tokens) == 1, f"Got {len(quote_tokens)} quote tokens in the pair universe, the pair universe is total {pair_universe.get_count()} pairs"
        return quote_tokens[0]

    def fetch_exchange_universe(self) -> ExchangeUniverse:
        return self.exchange_universe

    def fetch_pair_universe(self) -> Table:
        return self.pairs_table



