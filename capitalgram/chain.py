"""Information on EVM based chains."""
import enum


class ChainId(enum.Enum):
    """Ethereum EVM chain ids.

    See https://chainid.network/ for the full list.
    """
    ethereum = 1