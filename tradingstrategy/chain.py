"""Blockchain ids.

Data structures and information about EVM based blockchains.
Because the same trading pair and smart contract can across multiple blockchains,
we need to have a way to identify blockchains.
See :py:class:`ChainId` enum class for passing the identity of a blockchain around.
This is based on the underlying `web3.eth.chain_id` attribute of a chain.

Trading Strategy package embeds the chain list data from `chains repository <https://github.com/ethereum-lists/chains>`_
as the submodule for the Python package. This data is used to populate some :py:class:`ChainId`
data.
"""

import os
import enum
import json
from typing import Optional, Dict

#: In-process cached chain data, so we do not need to hit FS every time we access
_chain_data: Dict[int, dict] = {}

#: Slug to chain id mapping
_slug_map: Dict[str, int] = {}


class ChainDataDoesNotExist(Exception):
    """Cannot find data for a specific chain"""


def _ensure_chain_data_lazy_init():

    global _chain_data
    global _slug_map

    if _chain_data:
        # Already initialized
        return

    for chain_id in ChainId:
        chain_id = chain_id.value
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), "chains", "_data", "chains"))
        if not os.path.exists(path):
            raise RuntimeError(f"Chain data folder {path} not found. Make sure you have initialised git submodules or Python packaking is correct.\nHint: git submodule update --recursive --init")

        # Ganache does not have chain data entry
        dataless = _CHAIN_DATA_OVERRIDES.get(chain_id, {}).get("dataless", False)

        if not dataless:

            data_file = os.path.join(path, f"eip155-{chain_id}.json")
            if not os.path.exists(data_file):
                raise ChainDataDoesNotExist(f"Chain data does not exist: {data_file}")

            with open(data_file, "rt") as inp:
                _chain_data[chain_id] = json.load(inp)

        else:

            _chain_data[chain_id] = {}

        # Apply our own chain data records
        _chain_data[chain_id].update(_CHAIN_DATA_OVERRIDES.get(chain_id, {}))

    # Build slug -> chain id reverse mapping
    for chain_id, data in _chain_data.items():
        _slug_map[data["slug"]] = chain_id


def _get_chain_data(chain_id: int):
    _ensure_chain_data_lazy_init()
    return _chain_data[chain_id]


def _get_slug_map() -> Dict[str, int]:
    _ensure_chain_data_lazy_init()
    return _slug_map


class ChainId(enum.IntEnum):
    """Chain ids and chain metadata helper.

    This class is intended to present primary key for a blockchain in datasets,
    not its native chain id. This id may differ from what blockchain
    assumes its own id natively.

    Chain id is an integer that defines the identity of a blockchain,
    all running on same or different EVM implementations. For non-EVM
    blockchains we have some special logic to handle them so we can
    present chain ids or all blockchains through this enum.

    Chain id is a 32-bit integer.
    
    For non-EVM chains like ones on Cosmos or Solana we use negative values,
    so that they are not confused with chain.network database. Cosmos
    natively uses string ids for chains instead of integers.

    This class also provides various other metadata attributes besides `ChainId.value`, like `ChainId.get_slug()`.
    Some of this data is handcoded, some is pulled from `chains` submodule.

    For the full chain id list see:

    - `chainid.network <https://chainid.network/>`_

    - `chains repo <https://github.com/ethereum-lists/chains>`_

    = `Cosmos chain registry <https://github.com/cosmos/chain-registry
    """

    #: Ethereum mainnet chain id
    ethereum = 1

    #: Binance Smarrt Chain mainnet chain id
    bsc = 56

    #: Polygon chain id
    polygon = 137

    #: Avalanche C-chain id
    avalanche = 43114

    #: Ethereum Classic chain id.
    #: This is also the value used by EthereumTester in unit tests.
    #: https://github.com/ethereum/eth-tester
    ethereum_classic = 61

    #: Ganache test chain.
    #: This is the chain id for Ganache local tester / mainnet forks.
    ganache = 1337

    #: Chain id not known
    unknown = 0

    #: Osmosis on Cosmos
    #: Does not have chain registry entry,
    #: beacuse Cosmos maintains its own registry
    osmosis = -100

    @property
    def data(self) -> dict:
        """Get chain data entry for this chain."""
        return _get_chain_data(self.value)

    def get_name(self) -> str:
        """Get full human readab name for this blockchain"""
        return self.data["name"]

    def get_slug(self) -> str:
        """Get URL slug for this chain"""
        return self.data["slug"]

    def get_homepage(self) -> str:
        """Get homepage link for this blockchain"""

        # TODO: Use chain id JSON data in the future
        return self.data["infoURL"]

    def get_svg_icon_link(self) -> str:
        """Get an absolute SVG image link to a chain icon, transparent background"""
        return self.data["svg_icon"]

    def get_explorer(self) -> str:
        """Get explorer landing page for this blockchain"""
        return self.data["explorers"][0]["url"]

    def get_address_link(self, address) -> str:
        """Get one address link.

        Use EIP3091 format.

        https://eips.ethereum.org/EIPS/eip-3091
        """
        return f"{self.get_explorer()}/address/{address}"

    def get_tx_link(self, tx) -> str:
        """Get one tx link"""
        return f"{self.get_explorer()}/tx/{tx}"

    @staticmethod
    def get_by_slug(slug: str) -> Optional["ChainId"]:
        """Map a slug back to the chain.

        Most useful for resolving URLs.
        """
        slug_map = _get_slug_map()
        chain_id_value = slug_map.get(slug)
        if chain_id_value is None:
            return None
        return ChainId(chain_id_value)


#: Override stuff we do not like in Chain data repo
#:
#: Arweave permaweb dropped
#: https://hv4gxzchk24cqfezebn3ujjz6oy2kbtztv5vghn6kpbkjc3vg4rq.arweave.net/PXhr5EdWuCgUmSBbuiU587GlBnmde1MdvlPCpIt1NyM/
#: with free AR from the faucet https://faucet.arweave.net/
#:
_CHAIN_DATA_OVERRIDES = {
    1: {
        "name": "Ethereum",
        "slug": "ethereum",
        "svg_icon": "https://upload.wikimedia.org/wikipedia/commons/0/05/Ethereum_logo_2014.svg",
    },

    #
    # BSC
    #
    56: {
        "name": "Binance Smart Chain",
        # Deployed on Arweave for good
        "slug": "binance",
        "svg_icon": "https://hv4gxzchk24cqfezebn3ujjz6oy2kbtztv5vghn6kpbkjc3vg4rq.arweave.net/fgp9wHyH92hION8E6CuPtUNbmiTlqsl23QbQlwA8cZQ",
    },

    #
    # Polygon
    #
    137: {
        "name": "Polygon",
        "slug": "polygon",
        "svg_icon": "https://hv4gxzchk24cqfezebn3ujjz6oy2kbtztv5vghn6kpbkjc3vg4rq.arweave.net/nLW0IfMZnhhaqdN1AbzC4d1NLZSpBlIMEHhXq-KcOws",
    },

    #
    # Ethereum Classic / Ethereum Tester
    #
    61: {
        "name": "Ethereum Classic",
        "slug": "etc",
        "svg_icon": "https://upload.wikimedia.org/wikipedia/commons/0/05/Ethereum_logo_2014.svg",
        "active": False,
    },

    #
    # Ganache test chain
    #
    1337: {
        "name": "Ganache",
        "slug": "ganache",
        "svg_icon": None,
        "active": False,
        "dataless": True,
    },

    # 
    # Avalanche
    #
    43114: {
        "name": "Avalanche C-chain",
        "slug": "avalanche",
        "svg_icon": "https://cryptologos.cc/logos/avalanche-avax-logo.svg", # TODO
    },

    #
    # Osmosis
    #
    ChainId.osmosis.value: {
        "name": "Osmosis",
        "slug": "osmosis",
        "svg_icon": None,
        "active": False,
        "dataless": True,
    },

    #
    # Unknown
    #
    ChainId.unknown.value: {
        "name": "Unknown",
        "slug": "unknown",
        "svg_icon": None,
        "active": False,
        "dataless": True,
    },

}

