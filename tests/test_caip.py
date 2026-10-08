import pytest

from tradingstrategy.caip import BadAddress, ChainAddressTuple
from tradingstrategy.chain import ChainId


def test_caip_parse_naive():
    tuple = ChainAddressTuple.parse_naive("1:0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc")
    assert tuple.chain_id == 1
    assert tuple.address == "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"


@pytest.mark.skip(reason="Skipped because eth-utils dependency issues")
def test_caip_bad_checksum():
    # Notive lower b, as Ethereum encodes the checksum in the hex capitalisation
    with pytest.raises(BadAddress):
        ChainAddressTuple.parse_naive("1:0xb4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc")


def test_chain_name():
    c = ChainId(1)
    assert c.get_name() == "Ethereum"


def test_chain_homepage():
    c = ChainId(1)
    assert c.get_homepage() == "https://ethereum.org"


def test_bsc():
    c = ChainId(56)
    assert c.get_name() == "BNB Smart Chain"
    assert c.get_slug() == "binance"

    d = ChainId.binance
    assert d.get_name() == "BNB Smart Chain"
    assert d.get_slug() == "binance"


def test_avalanche():
    c = ChainId(43114)
    assert c.get_name() == "Avalanche C-chain"
    assert c.get_slug() == "avalanche"


def test_arbitrum():
    c = ChainId(42161)
    assert c.get_name() == "Arbitrum One"
    assert c.get_slug() == "arbitrum"


def test_resolve_by_slug():
    c = ChainId.get_by_slug("binance")
    assert c.value == 56

    c = ChainId.get_by_slug("arbitrum")
    assert c.value == 42161


def test_new_vault_chain_ids_from_eth_defi() -> None:
    """Support chain ids emitted by eth_defi vault metadata exports.

    1. Resolve Tempo, Robinhood and ApeX by their raw chain ids.
    2. Check their display names match the eth_defi chain metadata.
    3. Check slug lookup works for URL and metadata consumers.
    """
    # 1. Resolve Tempo, Robinhood and ApeX by their raw chain ids.
    tempo = ChainId(4217)
    robinhood = ChainId(4663)
    apex = ChainId(9995)

    # 2. Check their display names match the eth_defi chain metadata.
    assert tempo.get_name() == "Tempo"
    assert robinhood.get_name() == "Robinhood"
    assert apex.get_name() == "ApeX"

    # 3. Check slug lookup works for URL and metadata consumers.
    assert ChainId.get_by_slug("tempo") == tempo
    assert ChainId.get_by_slug("robinhood") == robinhood
    assert ChainId.get_by_slug("apex") == apex


def test_arc_plume_and_world_chain_metadata() -> None:
    """Resolve Arc, Plume and World Chain metadata when displaying imported vaults.

    1. Resolve all three chains from the numeric ids supplied by vault exports.
    2. Check names, slugs and reverse slug lookup.
    3. Check homepages, explorer links and optional icons.
    """
    # 1. Resolve all three chains from the numeric ids supplied by vault exports.
    arc = ChainId(5042)
    plume = ChainId(98866)
    world = ChainId(480)
    assert arc == ChainId.arc
    assert plume == ChainId.plume
    assert world == ChainId.world

    # 2. Check names, slugs and reverse slug lookup.
    assert arc.get_name() == "Arc"
    assert plume.get_name() == "Plume"
    assert world.get_name() == "World Chain"
    assert arc.get_slug() == "arc"
    assert plume.get_slug() == "plume"
    assert world.get_slug() == "world"
    assert ChainId.get_by_slug("arc") == arc
    assert ChainId.get_by_slug("plume") == plume
    assert ChainId.get_by_slug("world") == world

    # 3. Check homepages, explorer links and optional icons.
    assert arc.get_homepage() == "https://arc.io"
    assert plume.get_homepage() == "https://plume.org"
    assert world.get_homepage() == "https://world.org/world-chain"
    assert arc.get_address_link("0x123") == "https://explorer.arc.io/address/0x123"
    assert plume.get_tx_link("0x456") == "https://explorer.plume.org/tx/0x456"
    assert world.get_address_link("0x789") == "https://worldscan.org/address/0x789"
    assert arc.get_svg_icon_link() is None
    assert plume.get_svg_icon_link() is None
    assert world.get_svg_icon_link() is None
