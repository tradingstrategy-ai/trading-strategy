.. _trading data:

Trading data
============

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Preface
-------

Trading data feeds establish the foundation of any algorithmic trading. Here is information on Trading Strategy data feeds and datasets.

*Note*: This service is still in early beta and subject to change. Reach out in `Discord for any questions <https://tradingstrategy.ai/community>`_.

Available data
--------------

You can explore `the available data on Trading Strategy trading view pages <https://tradingstrategy.ai/trading-view/exchanges>`_.
You will find the lists of decentralised exchanges, trading pairs, price charts and metadata.

Available datasets
------------------

There are two categories of data

* `Real-time API <https://tradingstrategy.ai/api/explorer/>`_ for live trading and price charts

* `Backtesting datasets <https://tradingstrategy.ai/trading-view/backtesting>`_ for testing strategies

Access
------

Currently real-time API does not require authentication.
Backtesting data requires authentication by an API key due to large size of served files.

API key registration
~~~~~~~~~~~~~~~~~~~~

At the moment, the API key registration is only available through the interactive prompt on :doc:`Getting started <examples/getting-started>` notebook.

Downloading datasets by hand
-----------------------------

After you have obtained an API key `you can download datasets from the backteting page <https://tradingstrategy.ai/trading-view/backtesting>`_.

Accessing datasets programmatically
-----------------------------------

You should access `the datasets using the Python client library <https://pypi.org/project/tradingstrategy/>`_.
See :doc:`the first example Jupyter notebook how to do this <examples/getting-started>`.

If you wish not to use Python here are instructions how to construct dataset download URLs by hand.

API endpoints
~~~~~~~~~~~~~

Datasets can be downloaded over authenticated HTTPS API endpoints.

At the moment, all API endpoints are served by a private beta server `https://tradingstrategy.ai/api`.

APIs are described by Python dataclasses. For more information about the dataset data structure, see the API documentation and relevant source code.

Parquet endpoints
~~~~~~~~~~~~~~~~~

These take HTTP GET parameter `bucket`. See available values in :py:class:`tradingstrategy.timebucket.TimeBucket`.
They return a Parquet file download.

.. code-block:: none

    /pair-universe
    /candles-all
    /liquidity-all

JSON endpoints
~~~~~~~~~~~~~~

These do not take parameters. They return a JSON file download.

.. code-block:: none

    /exchanges

Downloading datasets programmatically
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All endpoints need your API key in the `Authorisation` header.

Because files are large, you need to stream them, as they are unlikely to fit to the RAM.

Example how to download:

.. code-block:: python

    import os
    import requests

    # Read API key from the process environment
    # should be in format "secret-token:tradingstrategy-48...
    # where the secret-token is the part of the API key itself
    api_key = os.environ["TRADING_STRATEGY_API_KEY"]

    session = requests.Session()
    session.headers.update({'Authorization': api_key})
    server = "https://tradingstrategy.ai/api"
    url = f"{server}/candles-all"
    params= {"bucket": "1d"}
    resp = session.get(url, allow_redirects=True, stream=True, params=params)
    resp.raise_for_status()
    size = 0
    with open('candles.parquet', 'wb') as handle:
        for block in resp.iter_content(64*1024):
            handle.write(block)
            size += len(block)

    print(f"Downloaded {size:,} bytes")


Reading datasets
~~~~~~~~~~~~~~~~

Datasets are distributed as compressed :term:`Parquet` files, using Parquet version 2.0.

You can read files using PyArrow:

.. code-block:: python

    import pyarrow as pa
    from pyarrow import parquet as pq

    table: pa.Table = pq.read_table("candles.parquet")

Then, you can directly import the table to your database or convert the table to Pandas DataFrame for further manipulation.