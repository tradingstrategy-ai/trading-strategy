import enum
from typing import Optional


class TradeHintType(enum.Enum):
    """Add hints to the backtested trades.

    This help later to analyse the backtested trades and differ e.g. stop losses from normal position closes.
    """

    open = "open"
    close = "close"
    stop_loss_triggered = "stop_loss_triggered"


class TradeHint:

    def __init__(self, type: TradeHintType, message: Optional[str]=None):
        self.type = type
        self.message = message