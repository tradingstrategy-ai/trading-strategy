"""Data structures and information about EVM based blockchains."""

import enum


class ChainId(enum.Enum):
    """Ethereum EVM chain ids.

    Chain id is an integer that defines the identity of a blockchain,
    all running on same or different EVM implementations.

    See https://chainid.network/ for the full list.
    """

    #: Ethereum mainnet chain id
    ethereum = 1