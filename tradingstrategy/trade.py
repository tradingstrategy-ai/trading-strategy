"""Individual DEX trade data."""
from dataclasses import dataclass


@dataclass
class Trade:
    """Individual trade.

    Describe the data structure used for individual trades in DEX trades dataset.

    - Dataset contains all supported DEX trade across supported blockchains

    - Based on emitted Solidity events

    - Data can be cross-referenced to blockchain transactions and log indexed

    - Trades are Swaps for Uniswap

    - Dataset focuses on executed price, for market price feed
      see candle datasets

    - Trading pairs are normalised to base token - quote token
      human-readable format, as opposite to raw Uniswap token pairs

    - The approximate best effort USD exchange rate is included,
      when available, to calculate US dollar measured impact of trades

    - Dataset is sorted for the best file compression - trades
      are not guaranteed to be ordered by time
    """

