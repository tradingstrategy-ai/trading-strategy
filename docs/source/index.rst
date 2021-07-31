.. Capitalgram documentation master file, created by
   sphinx-quickstart on Thu Jul 15 09:33:58 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Capitalgram documentation
=========================

Capitalgram is an :term:`on-chain` quantative finance framework and trade automation protocol. You can develop and :term:`backtest` trading strategies based on all trades ever happened on any blockchain. Live data is available to one minute accuracy.

The ready strategies, or trading bots, can be deployed as :term:`autonomous agents <autonomous agent>` running on :term:`smart contracts <smart contract>`. Strategies then trade on :term:`decentralised exchanges <decentralised exchange>`. After deployed, anyone can invest in and withdraw from the strategies in real time.

Capitalgram integrates with :term:`Jupyter notebook`, :term:`Pandas`, :term:`Backtrader` and other popular Python based quantative finance libraries.

Build on
--------

.. raw:: html

   <img class="logo" src="_static/logos/ethereum.png">

   <img class="logo logo-smaller" src="_static/logos/python.png">

   <img class="logo" src="_static/logos/pandas.png">

   <img class="logo" src="_static/logos/pyarrow.png">

   <img class="logo" src="_static/logos/colab.png">

Community
---------

`Github repository <https://github.com/miohtama/capitalgram-onchain-dex-quant-data>`_.

Documentation
-------------

Find the documentation for research notebooks and Python APIs below.

.. toctree::
   :maxdepth: 1
   :caption: Narrative documentation

   examples/getting-started
   running
   learn
   development
   glossary

.. toctree::
   :maxdepth: 1
   :caption: Code examples

   examples/plotting
   examples/technical-analysis
   examples/pairs
   examples/fastquant

.. toctree::
   :maxdepth: 1
   :caption: Algorithms

   algorithms/entropy-monkey

.. toctree::
   :maxdepth: 1
   :caption: Trade execution

   execution/overview
   execution/deploy

.. toctree::
   :maxdepth: 1
   :caption: API documentation

   api/client
   api/reader
   api/exchange
   api/pair
   api/candle
   api/liquidity
   api/chain
   api/timebucket
   api/caip
   api/types
   api/backtrader
   api/matplotlib
   api/fastquant

