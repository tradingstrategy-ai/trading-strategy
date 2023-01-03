"""Synthetic random price data feed for unit testing."""

import random
from decimal import Decimal
from typing import Dict, Optional, List, Iterable, Type

import pandas as pd
from tqdm import tqdm

from eth_defi.event_reader.block_header import BlockHeader
from eth_defi.price_oracle.oracle import BasePriceOracle
from eth_defi.event_reader.reorganisation_monitor import ReorganisationMonitor

from .timeframe import Timeframe
from .trade_feed import TradeFeed, Trade
from .direct_feed_pair import PairId


class SyntheticTradeFeed(TradeFeed):
    """Synthetic trade feed that produces random trades."""

    def __init__(self,
                 pairs: List[PairId],
                 oracles: Dict[PairId, BasePriceOracle],
                 reorg_mon: ReorganisationMonitor,
                 data_retention_time: Optional[pd.Timedelta] = None,
                 random_seed = 1,
                 start_price_range=150,
                 end_price_range=300,
                 min_trades_per_block=0,
                 max_trades_per_block=10,
                 min_amount=50,
                 max_amount=50,
                 price_movement_per_trade=2.5,
                 timeframe: Timeframe = Timeframe("1min"),
                 prices: Dict[PairId, float] = None,
                 ):
        super().__init__(
            pairs=pairs,
            oracles=oracles,
            reorg_mon=reorg_mon,
            data_retention_time=data_retention_time,
            timeframe=timeframe,
        )

        self.pairs = pairs
        self.min_trades_per_block = min_trades_per_block
        self.max_trades_per_block = max_trades_per_block
        self.min_amount = min_amount
        self.max_amount = max_amount
        self.price_movement_per_trade = price_movement_per_trade
        self.random_gen = random.Random(random_seed)

        if not prices:
            self.prices = {
                p: self.random_gen.randint(start_price_range, end_price_range)
                for p in pairs
            }
        else:
            self.prices = prices

    def fetch_trades(self, start_block: int, end_block: Optional[int], tqdm: Optional[Type[tqdm]] = None) -> Iterable[Trade]:
        """Generate few random trades per block per pair."""

        block_data = {b.block_number: b for b in self.reorg_mon.fetch_block_data(start_block, end_block)}

        max_blocks = end_block - start_block

        if max_blocks > 5 and tqdm:
            progress_bar = tqdm(total=max_blocks)
        else:
            progress_bar = None

        for block_num in range(start_block, end_block + 1):
            for p in self.pairs:
                trades_per_block = self.random_gen.randint(self.min_trades_per_block, self.max_trades_per_block)
                for trade_idx in range(trades_per_block):

                    self.prices[p] += self.random_gen.uniform(-self.price_movement_per_trade, self.price_movement_per_trade)
                    self.prices[p] = max(self.prices[p], 0.00001)  # Don't go to negative prices
                    price = self.prices[p]
                    amount = self.random_gen.uniform(self.min_amount, self.max_amount)

                    block: BlockHeader = block_data[block_num]
                    tx_hash = hex(self.random_gen.randint(2**31, 2**32))
                    log_index = trade_idx

                    exchange_rate = self.get_exchange_rate(p)

                    yield Trade(
                           pair=p,
                           block_number=block_num,
                           block_hash=block.block_hash,
                           timestamp=pd.Timestamp.utcfromtimestamp(block.timestamp),
                           tx_hash=tx_hash,
                           log_index=log_index,
                           price=Decimal(price),
                           amount=Decimal(amount),
                           exchange_rate=exchange_rate,
                        )

            if progress_bar:
                progress_bar.update(1)

        if progress_bar:
            progress_bar.close()
