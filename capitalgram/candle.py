from dataclasses import dataclass


from capitalgram.caip import ChainAddressTuple
from capitalgram.chain import ChainId
from capitalgram.units import NonChecksummedAddress, UNIXTimestamp, USDollarAmount, BlockNumber


@dataclass
class Candle:
    """DEX trade candle.

    OHLCV candle with some extra information available,
    as our chain analysis allows deeper visibility that you would
    get with traditional exchanges.
    """
    chain_id: ChainId  # 1 for Ethereum
    address: NonChecksummedAddress  # Pair contract address

    #: Open timestamp for this candle.
    #: Note that the close timestamp you need to supply yourself based on the context.
    timestamp: UNIXTimestamp  # UNIX timestamp as seconds

    #: USD exchange rate of the quote token used to
    #: generate any dollar amount
    exchange_rate: float

    #: OHLCV core data
    open: USDollarAmount
    close: USDollarAmount
    high: USDollarAmount
    low: USDollarAmount

    #: Number of buys happened during the candle period
    buys: int  # Numb

    #: Number of sells happened during the candle period
    sells: int

    #: Volume data.
    #: Note that we separate buys and sells
    buy_volume: USDollarAmount
    sell_volume: USDollarAmount

    #: Average trade size
    avg: USDollarAmount

    #: Blockchain tracking information
    start_block: BlockNumber
    end_block: BlockNumber

    @property
    def caip(self) -> ChainAddressTuple:
        """Unique identifier for the trading pair"""
        return ChainAddressTuple(self.chain_id.value, self.address)

    @property
    def trades(self):
        """Amount of all trades during the candle period."""
        return self.buys + self.sells

    @property
    def volume(self):
        """Total volume during the candle period."""
        return self.buy_volume + self.sell_volume

    def __json__(self):
        """Pyramid JSON renderer compatible transformer."""
        return self.__dict__

