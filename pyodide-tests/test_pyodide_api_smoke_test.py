"""Check we manage to import and create API client under Pyodide.

TODO: Much work in progress

To run:

.. code-block:: console

    scripts/run-pyodide-tests.sh

"""

import shutil
from pathlib import Path

import pytest
from poetry.core.factory import Factory
from poetry.core.masonry.builders.wheel import WheelBuilder

try:
    from pytest_pyodide import run_in_pyodide, spawn_web_server
    HAS_PYODIDE = True
except ImportError:
    HAS_PYODIDE = False


# Run tests only if chromedriver is installed
pytestmark = pytest.mark.skipif(
    shutil.which("chromedriver") is None and not HAS_PYODIDE,
    reason="chromedriver and pytest-pyodide need to be installed in order to run pyodide tests")


@pytest.fixture(scope="session")
def dist_path() -> Path:
    """Path to dist/pyodide.

    This is where

    - We have untarred Pyodide distribution

    - Our custom build wheels go
    """
    dist_path = Path("dist").joinpath("pyodide")
    assert dist_path.exists()
    return dist_path


@pytest.fixture(scope="session")
def our_package_pyodide_build(dist_path) -> str:
    """Package the current development code to Pyodide wheel.

    This fixture allows `trading-strategy` package to be installed
    within Pyodide context, as a micropip package.

    - Run the package build for the current development Python code

    - Place it in the dist, so that it is accessible in Pyodide run-time

    :return:
        The path to the build wheel file
    """

    # https://github.com/python-poetry/poetry-core/blob/main/tests/masonry/builders/test_wheel.py#L54
    poetry = Factory().create_poetry(Path("."))
    builder = WheelBuilder(poetry)
    builder.build(dist_path)
    wheel_file = builder.wheel_filename
    full_path = dist_path.joinpath(wheel_file)
    assert full_path.exists()
    return wheel_file


@pytest.fixture(scope="session")
def pyarrow_dist(dist_path) -> str:
    """Drop hand-build Pyarrow module to dist.

    Currently Pyodide core distribution does not include PyArrow yet
    which is a core dependency for us to read Parquet files.
    Drop a hand-built wheel we can use to get our package installed.

    For the wheel source, see here https://github.com/tradingstrategy-ai/pyarrow-wasm/
    """
    name = "pyarrow-8.0-py3-none-any.whl"
    #shutil.copy(f"extras/{name}", dist_path)
    return name


@run_in_pyodide
def test_pyodide_smoke(selenium):
    """Does Pyodide work at all?"""
    assert 1 + 1 == 2


@run_in_pyodide
def test_pyodide_document(selenium):
    """Access JS document scope"""
    # https://pyodide.org/en/stable/usage/type-conversions.html?highlight=window#importing-javascript-objects-into-python
    import js
    assert js.document.title == "pyodide"




# See https://github.com/pyodide/pyodide/discussions/3100
@pytest.mark.skip(msg="Currently does not work")
def test_pyodide_create_client(
        selenium_standalone,
        dist_path,
        pyarrow_dist,
        our_package_pyodide_build):
    """Create Trading Strategy client in Pyodide context.

    - Downloads trading-strategy wheel from built-in web server

    - Runs micropip install

    - Attempts to run Python code after micropip install is complete

    See example here

    - https://github.com/pyodide/pytest-pyodide/blob/main/examples/test_install_package.py#L18
    """

    with spawn_web_server(dist_path.absolute()) as server:
        server_hostname, server_port, _ = server
        base_url = f"http://{server_hostname}:{server_port}/"

        pyarrow_url = base_url + pyarrow_dist
        url = base_url + our_package_pyodide_build
        numpy_url = base_url + "numpy-1.22.4-cp310-cp310-emscripten_3_1_14_wasm32.whl"

        selenium = selenium_standalone
        selenium.run_js(
            f"""                        
            await pyodide.loadPackage("micropip");
            const micropip = pyodide.pyimport("micropip");
            // await micropip.install('{url}');              
            await micropip.install('{pyarrow_url}');                        
            """
        )
        selenium.run(
            """
            from tradingstrategy.client import Client
            client = Client.create_pyodide_client()
            """
        )
