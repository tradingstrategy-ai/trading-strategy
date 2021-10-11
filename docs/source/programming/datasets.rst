.. _trading data:

Trading data
============

.. toctree::
   :maxdepth: 2
   :caption: Contents:

List of available DEX datasets
------------------------------

Trading Strategy provides real-time and historical DEX trading data as :term:`datasets <dataset>`.
See `the available datasets <http://localhost:3000/trading-view/backtesting>`__.

API endpoints
-------------

Datasets can be downloaded over authenticated HTTPS API endpoints.

At the moment, all API endpoints are served by a private beta server `https://candlelightdinner.tradingstrategy.ai`.

APIs are described by Python dataclasses. For more information about the dataset data structure, see the API documentation and relevant source code.

API key registration
--------------------

At the moment, the API key registration is only available through the interactive prompt on :doc:`Getting started <examples/getting-started>` notebook.

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

Downloading datasets
--------------------

All endpoints need your API key in the `Authorisation` header.

Because files are large, you need to stream them, as they are unlikely to fit to the RAM.

Example how to download:

.. code-block:: python

    import os
    import requests

    # Read API key from the process environment
    api_key = os.environ["CAPITALGRAM_API_KEY"]

    session = requests.Session()
    session.headers.update({'Authorization': api_key})
    server = "https://candlelightdinner.capitalgram.com"
    url = f"{server}/candles-all"
    params= {"bucket": "1d"}
    resp = session.get(url, allow_redirects=True, stream=True, params=params)
    resp.raise_for_status()
    with open('candles.parquet', 'wb') as handle:
        for block in response.iter_content(64*1024):
            handle.write(block)

Reading datasets
----------------

Datasets are distributed as compressed :term:`Parquet` files, using Parquet version 2.0.

You can read files using PyArrow:

.. code-block:: python

    import pyarrow as pa
    from pyarrow import parquet as pq

    table: pa.Table = pq.read_table("candles.parquet")

Then, you can directly import the table to your database or convert the table to Pandas DataFrame for further manipulation.