Development
===========

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Preface
-------

In this chapter, we will discuss how to use `tradingstrategy` locally with your Python or Jupyter Notebook to development new strategies or the library itself.

Installation for developer
--------------------------

To install Trading Strategy using pip do:

.. code-block:: shell

    git clone # Get clone from the Github ...
    cd tradingstrategy
    poetry shell
    poetry install -E qstrader

Run tests
---------

To run tests you need to have a Trading Strategy API key. Tests use the production server.

.. code-block:: shell

    poetry shell
    export TRADING_STRATEGY_API_KEY="secret-token:tradingstrategy-xxx"
    pytest

Tests are very slow.

By default, the test run will cache any downloaded blobs. You can force the redownload with:

.. code-block:: shell

    CLEAR_CACHES=true pytest --log-cli-level=debug -k test_grouped_liquidity


Dataset cache
-------------

The default cache location for the downloaded datasets is `~/.cache/tradingstrategy`.

.. code-block:: shell

    ls -lha ~/.cache/tradingstrategy

.. code-block:: text

    total 56M
    drwxr-xr-x  5 moo staff  160 Jul 19 23:14 ./
    drwx------ 14 moo staff  448 Jul 18 15:49 ../
    -rw-r--r--  1 moo staff  49M Jul 19 23:14 candles-24h.feather
    -rw-r--r--  1 moo staff  95K Jul 18 15:49 exchange-universe.json
    -rw-r--r--  1 moo staff 6.3M Jul 19 21:57 pair-universe.json.zstd


You can clear this out manually from the UNIX shell

.. code-block:: shell

    rm -rf ~/.cache/tradingstrategy


Deploying new documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Github Actions workflow will deploy on Netlify. You need Netlify `AUTH_TOKEN` and `SITE_ID`.

`AUTH_TOKEN` can be generated in the user settings.

Making a release
----------------

`Release with poetry <https://python-poetry.org/docs/cli/>`_.

.. code-block:: shell

    poetry build
    poetry publish