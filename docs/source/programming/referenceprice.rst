Reference pricing
==================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Introduction
------------

Trading Strategy converts all trading pair quotes to US dollars.

* US dollar prices are more human readable, making analysing the market
  and comparing the prices and volumes easier

* As the service is geared towards algorithmic trading, only the direction of the price matters,
  not its precise value

Internal oracle process
-----------------------

Here is the description of the current reference price process, known as reference price v0.

**This process is not final and not very safe in its current form and only applies to the beta version of Trading Strategy**.

Currently the US dollar conversion works as is

* All reference price sources must be on-chain, to keep the data pure (exception: BTC/USD comes from Bitstamp).

* All reference prices must be specific to the chain where the asset trades. E.g.
  Binance Smart Chain trading pairs can only have quote tokens of which reference price comes from a pool
  on Binance Smart Chain itself.

* Reference price is generated as as a separate step before OHLCV candle data, as OHLCV data generation requires to have
  a good US dollar exchange rates.

* Reference price is generated from known quote token - US dollar pools.

* Price is formed from 1 minute swaps and liquidity events of the pool.
  If not trades have been done, the latest available on-chain trade event is
  used as the reference price.

Currently reference price is supported for following tokens

* ETH on Ethereum mainnet (ETH/USDC Uniswap v2 pool on Ethereum mainnet)

* BNB

* MATIC

* Cake

* QUICK

Notes
-----

About price oracle security
~~~~~~~~~~~~~~~~~~~~~~~~~~~

`Read this post about spot price security when used in an oracle <https://ethereum.stackexchange.com/a/114990/620>`_.