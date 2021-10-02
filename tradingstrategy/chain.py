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

    def get_name(self) -> str:
        """Get full human readab name for this blockchain"""
        return self.name.title()

    def get_homepage(self) -> str:
        """Get homepage link for this blockchain"""

        # TODO: Use chain id JSON data in the future
        return "https://ethereum.org"

    def get_explorer(self) -> str:
        """Get explorer landing page for this blockchain"""
        return "https://etherscan.io"

    def get_address_link(self, address) -> str:
        """Get one address link"""
        return f"https://etherscan.io/address/{address}"

    def get_tx_link(self, tx) -> str:
        """Get one tx link"""
        return f"https://etherscan.io/tx/{tx}"

