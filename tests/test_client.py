"""Client dataset download and integrity tests"""

import os
import json
import logging
from pathlib import Path
from textwrap import dedent

import pytest

from tradingstrategy.environment.config import Configuration
from tradingstrategy.environment.default_environment import DefaultClientEnvironment, SettingsDisabled
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.client import Client
from tradingstrategy.chain import ChainId
from tradingstrategy.pair import LegacyPairUniverse


logger = logging.getLogger(__name__)

CI = os.environ.get("CI") == "true"


def test_client_ping(client: Client):
    """Unauthenticated ping"""
    data = client.transport.ping()
    assert data["ping"] == "pong"


def test_client_motd(client: Client):
    """Authenticated ping"""
    data = client.transport.message_of_the_day()
    assert "version" in data
    assert "message" in data


def test_client_fetch_chain_status(client: Client):
    """Get chain scanning status"""
    status = client.fetch_chain_status(ChainId.ethereum)
    assert status["chain_id"] == 1

    # TODO: The blockchain pair count is temporarily disabled for performance reasons,
    # and the chain data API just returns zero (0). Re-enable the assertion below once
    # the API is fixed.
    #
    # https://github.com/tradingstrategy-ai/oracle/commit/1dac3e1ef3c84ae7b6509242d114d4f6d1ae384a
    # assert status["pairs"] > 0

    # Removed as too slow to compute on the server-side for now
    # assert status["swaps"] > 0
    # assert status["minute_candles"] > 0
    # assert status["first_swap_at"] == '2020-05-05T21:09:32'


def test_client_download_exchange_universe(client: Client, cache_path: str):
    """Download exchange mapping data"""
    universe = client.fetch_exchange_universe()
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/exchange-universe.json")
    # Check universe has data
    assert len(universe.exchanges) > 0
    assert universe.exchanges[1].name == "Uniswap v2"
    assert universe.exchanges[1].exchange_slug == "uniswap-v2"
    assert universe.exchanges[1].chain_slug == "ethereum"
    assert universe.exchanges[1].address == "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"
    exchange = universe.get_by_chain_and_slug(ChainId.ethereum, "sushi")
    assert exchange.name == "Sushi"
    assert exchange.address == "0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac"
    exchange = universe.get_by_chain_and_name(ChainId.ethereum, "Shiba Swap")
    assert exchange.name == "Shiba Swap"
    assert exchange.exchange_slug == "shiba-swap"


@pytest.mark.skipif(CI, reason="Files are too large to handle on CI")
def test_client_download_all_pairs(client: Client, cache_path: str):
    """Download all candles for a specific candle width."""
    df = client.fetch_all_candles(TimeBucket.d30)
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/candles-30d.parquet")
    assert len(df) > 100


@pytest.mark.skipif(CI, reason="Files are too large to handle on CI")
def test_client_download_all_liquidity_samples(client: Client, cache_path: str):
    """Download all liquidity samples for a specific candle width."""
    df = client.fetch_all_liquidity_samples(TimeBucket.d30)
    # Check we cached the file correctly
    assert os.path.exists(f"{cache_path}/liquidity-samples-30d.parquet")
    assert len(df) > 100


@pytest.mark.skipif(CI, reason="Files are too large to handle on CI")
def test_client_convert_all_pairs_to_pandas(client: Client, cache_path: str):
    """We can convert the columnar Pyarrow data to Pandas format.

    This has some issues with timestamps, so adding a test.
    """
    pairs_table = client.fetch_pair_universe()
    df = pairs_table.to_pandas()
    assert len(df) > 1000


# Not yet supported
#
#@pytest.mark.asyncio
#async def test_create_pyodide_client_indexdb():
#    """Test the special client used in Pyodide which use IndexDB to save the API key."""
#    # https://tonybaloney.github.io/posts/async-test-patterns-for-pytest-and-unittest.htmlpy
#    env = JupyterEnvironment()
#    env.clear_configuration()
#    client = await Client.create_pyodide_client_async(remember_key=False)
#    assert isinstance(client, Client)


def test_create_pyodide_client_detect(tmp_path):
    """Test the special client used in Pyodide which use HTTP referral authentication."""
    env = DefaultClientEnvironment(settings_path=tmp_path)
    env.clear_configuration()
    client = Client.create_jupyter_client(pyodide=True, settings_path=tmp_path)

    # See we can import important modules
    # Before we had:
    # ModuleNotFound: eth_defi when importing lending
    import tradingstrategy.utils.token_filter
    import tradingstrategy.lending
    import tradingstrategy.stablecoin
    import tradingstrategy.utils.wrangle
    import tradingstrategy.pair
    import tradingstrategy.candle
    import tradingstrategy.liquidity
    import tradingstrategy.clmm
    import tradingstrategy.exchange
    assert isinstance(client, Client)



@pytest.mark.skipif(os.environ.get("TRADING_STRATEGY_API_KEY") is None, reason="Set TRADING_STRATEGY_API_KEY environment variable to run this test")
def test_settings_disabled():
    """We get an exception if settings are disabled when we try to access/create settings file."""

    api_key = os.environ["TRADING_STRATEGY_API_KEY"]

    client = Client.create_live_client(api_key=api_key, settings_path=None)
    env = client.env
    assert isinstance(env, DefaultClientEnvironment)
    with pytest.raises(SettingsDisabled):
        env.setup_on_demand()


def test_vault_pro_api_key_persist_reuse_and_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """needs_vault_data resolves the Vaults Pro key, persists it and reuses it, overriding stale keys.

    Offline: a base API key is supplied and no dataset is downloaded, so key
    resolution and persistence run without hitting the server.

    1. Seed settings.json with a stale base key and no vault key.
    2. Create a client with a new base key and VAULT_PRO_API_KEY set; check the
       resolved key and that both keys are persisted, overriding the stale one.
    3. Create the client again with the environment variable removed and check the
       vault key is reused from settings.json without prompting.
    """
    env = DefaultClientEnvironment(settings_path=tmp_path)

    # 1. Seed settings.json with a stale base key and no vault key.
    env.save_configuration(Configuration(api_key="secret-token:tradingstrategy-OLD"))

    # 2. Create a client with a new base key and the environment variable set.
    monkeypatch.setenv("VAULT_PRO_API_KEY", "creem-test-key-123456")
    client = Client.create_jupyter_client(
        api_key="secret-token:tradingstrategy-NEW",
        settings_path=tmp_path,
        needs_vault_data=True,
    )
    assert client.vault_pro_api_key == "creem-test-key-123456"
    config = env.discover_configuration()
    assert config.api_key == "secret-token:tradingstrategy-NEW"  # stale base key overridden
    assert config.vault_pro_api_key == "creem-test-key-123456"  # vault key persisted alongside it

    # 3. Create the client again with the environment variable removed.
    monkeypatch.delenv("VAULT_PRO_API_KEY")
    reused = Client.create_jupyter_client(
        api_key="secret-token:tradingstrategy-NEW",
        settings_path=tmp_path,
        needs_vault_data=True,
    )
    assert reused.vault_pro_api_key == "creem-test-key-123456"  # reused from settings.json


def test_vault_pro_api_key_absent_without_flag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Without needs_vault_data the client carries no Vaults Pro key and never prompts.

    1. Ensure no Vaults Pro key is present in the environment.
    2. Create a client without needs_vault_data and check it has no vault key.
    """
    # 1. Ensure no Vaults Pro key is present in the environment.
    monkeypatch.delenv("VAULT_PRO_API_KEY", raising=False)

    # 2. Create a client without needs_vault_data and check it has no vault key.
    client = Client.create_jupyter_client(
        api_key="secret-token:tradingstrategy-basekey",
        settings_path=tmp_path,
    )
    assert client.vault_pro_api_key is None


def test_notebook_interactive_vault_pro_prompt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Running a notebook with needs_vault_data interactively asks for the Vaults Pro key.

    Exercises the notebook code path end to end in a real Jupyter kernel: with no
    key configured it must reach the interactive prompt and persist the answer to
    the overridden temporary settings file, never the developer's real one.

    1. Point the settings file at a temporary directory and clear the environment
       variable, so neither the real settings file nor an ambient key is used.
    2. Execute a one-cell notebook that mocks input() and creates a Jupyter client
       with needs_vault_data=True.
    3. Check the notebook reached the prompt and persisted the key only into the
       temporary settings file.
    """
    nbformat = pytest.importorskip("nbformat")
    pytest.importorskip("nbclient")
    pytest.importorskip("ipykernel")
    from nbclient import NotebookClient
    from jupyter_client.kernelspec import KernelSpecManager

    try:
        KernelSpecManager().get_kernel_spec("python3")
    except Exception:
        pytest.skip("No python3 Jupyter kernel available for the integration test")

    # 1. Point the settings file at a temporary directory and clear the environment variable.
    settings_dir = tmp_path / "tradingstrategy"
    monkeypatch.setenv("TS_TEST_SETTINGS_DIR", str(settings_dir))
    monkeypatch.delenv("VAULT_PRO_API_KEY", raising=False)

    # 2. Execute a one-cell notebook that mocks input() and creates a client.
    source = dedent(
        '''
        import os
        import json
        import builtins
        from pathlib import Path
        from tradingstrategy.client import Client

        # Override the settings file location so the real ~/.tradingstrategy is untouched.
        settings_dir = Path(os.environ["TS_TEST_SETTINGS_DIR"])
        os.environ.pop("VAULT_PRO_API_KEY", None)  # force the interactive prompt
        assert not (settings_dir / "settings.json").exists()

        prompts = []
        def fake_input(prompt=""):
            prompts.append(prompt)
            return "creem-notebook-key-abcdef"
        builtins.input = fake_input

        client = Client.create_jupyter_client(
            api_key="secret-token:tradingstrategy-basekey",
            settings_path=settings_dir,
            needs_vault_data=True,
        )

        assert any("Vaults Pro API key" in p for p in prompts), f"No interactive prompt: {prompts}"
        assert client.vault_pro_api_key == "creem-notebook-key-abcdef"
        saved = json.loads((settings_dir / "settings.json").read_text())
        assert saved["vault_pro_api_key"] == "creem-notebook-key-abcdef"
        assert saved["api_key"] == "secret-token:tradingstrategy-basekey"
        '''
    )
    notebook = nbformat.v4.new_notebook()
    notebook.cells = [nbformat.v4.new_code_cell(source)]
    NotebookClient(notebook, timeout=120, kernel_name="python3").execute()

    # 3. Check the key was persisted only into the temporary settings file.
    settings_file = settings_dir / "settings.json"
    assert settings_file.exists()
    saved = json.loads(settings_file.read_text())
    assert saved["vault_pro_api_key"] == "creem-notebook-key-abcdef"