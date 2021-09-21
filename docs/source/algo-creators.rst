For algorithm vendors
=====================

If you are an algorithmic trading strategy programmer, systematic trader or a quantitative crypto hedge fund, you can earn extra with Trading Strategy.

.. raw:: html

   <img class="intro" src="_static/how-to-create-algorithmic-trading-strategy.png" alt="How to create an algorithmic trading strategy for cryptocurrencies">


How to create a strategy
------------------------

Trading Strategy offers decentralised finance :ref:`trading data` as the starting point for the strategy development. Register an API key and start exploring the datasets.

Plan your strategy type: Single pair strategy or portfolio optimisation strategy. Based on this Trading Strategy offers you too popular Python backtesting and trading framework to choose from.

* :ref:`Backtrader` for single-pair strategies, or strategies that trade few well-known trading pairs

* :ref:`QSTrader` for portfolio optimisations strategies.

* You can also do backtesting in your own tool and on your own infrastructure

Backtest your strategy using Python, as based on Trading Strategy provided code examples. Then, write your live trading strategy script.

Trading Strategy governance accept your request for a new algorithm, with proposed fee structure, and deploys a corresponding smart contract.

Trading can start.

Strategies can be hidden and redeployed until it is guaranteed the live execution works.

Strategy privacy and trade secrets
----------------------------------

Trading Strategy protocol strategies can be either private or public. In the private strategies, the algorithm creators are responsible for running their own servers.

Private strategies
~~~~~~~~~~~~~~~~~~

You can choose to keep your algorithm source code secret. In this case, you are responsible to run the oracle server yourself that sends the trading instructions to the smart contract.

Private strategies can contain trade secrets ("the secret sauce"). They can be complex. Private strategy vendor can choose to whitelist the participants that are allowed to invest in their strategy.

Public strategies
~~~~~~~~~~~~~~~~~

Public strategy algorithm source code is open and executed by the public Trading Strategy oracle server fleet.

Public strategies can be

* Simple strategies, like moving average based

* Schoolbook strategies

The public strategies aim to offer simple, well-known and battle-tested strategies, allowing investors to invest in them easily. They are unlikely to outperform top quant fund strategies, but will still offer better risk/return than the :term:`risk-free rate` on decentralised finance.

Earning potential and fees
--------------------------

The fees earned from the strategy are split between the algorithm vendor and other ecosystem participants. Any strategy has a flexible fee structure.

For more information read about :ref:`fee structures`.

Below are some benchmarks for fees and earnings

* Yield farming, like `Yearn Finance <https://yearn.finance/>`_ charges 20% performance fee and 2% management fee. Yield Finance offers compounding :term:`risk-free rate` yield farming strategies. `More information about Yearn Finance fees <https://docs.yearn.finance/yearn-finance/yvaults/overview>`_. The strategy creator can choose their revenue split that must be accepted by Yearn governance. `Typically this is 10% <https://academy.ivanontech.com/blog/yearn-finance-what-are-yearn-vaults>`_.

* Top quantitative Cayman island based crypto hedge funds charge 40% performance fee, no other fees.

* Liechtenstein based crypto investment funds, like an index fund and algo fund, charge 2% management fee and 20% performance fee.



