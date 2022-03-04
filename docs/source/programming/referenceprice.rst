US dollar price conversion
==========================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Introduction
------------

Each trading pair consists of two tokens

* Base token e.g. AAVE: (the token you are going to buy)

* Quote token e.g. ETH (the token you are going to use as the payment)

A lot of decentralised exchange trades happens against these non-fiat quote tokens like ETH and BNB.

Trading Strategy converts all trading pair quotes to US dollars.
This mechanism is called *reference pricing*.

* US dollar prices are more human readable, making analysing the market
  and comparing the prices and volumes easier

* As the service is geared towards algorithmic trading, only the direction of the price matters,
  not its precise value

The downside of the reference pricing is that you need to have a source of truth for
what's the underlying price of the quote token in US dollars. This includes additional
complexity to the internal processing for the benefit of a better user experience.
It also adds another source of risk, as pricing information may not be correctly
determined in the high volatility market movement.

Not having a good US dollar conversion path also affects to <>

Internal oracle process
-----------------------

Here is the description of the current reference price process, known as the reference price ``v0``.

.. danger::
  This process is not final and not very safe in its current form and only applies to the beta version of Trading Strategy.

Currently the US dollar conversion works as follows:

* All reference price sources must be on-chain, to keep the data pure (exception: BTC/USD comes from Bitstamp).

* All reference prices must be specific to the chain where the asset trades. E.g.
  Binance Smart Chain trading pairs can only have quote tokens of which reference price comes from a pool
  on Binance Smart Chain itself.

* Reference price is generated as a separate step before OHLCV candle data, as OHLCV data generation requires to have
  good US dollar exchange rates.

* Reference price is generated from known quote tokens - US dollar pools.

* Price is formed from 1 minute swaps and liquidity events of the pool.
  If no trades have been done, the latest available on-chain trade event is
  used as the reference price.

Currently reference price is supported for the following tokens:

* ETH on Ethereum mainnet (ETH/USDC Uniswap v2 pool on Ethereum mainnet)

* BNB

* MATIC

* Cake

* QUICK

Determining quote token
-----------------------

Uniswap v2 compatible exchanges do not care which token is a base token and which token is a quote token -
it only cares about token inputs and token outputs. Sell is just a buy with a reverse token order.

For making the market data analysis easier, all trading pairs on Trading Strategy are converted to
the form where we have an easier to understand ``base - quote`` token pair.

The quote token is always determined to be a well-known token. Currently we use the following
priority to determine which of the trading pairs tokens is a quote token.

* An USD stablecoin (**beta note**: any stablecoin is assumed to be 1:1 with US dollar)

* BTC

* ETH

* BNB

* MATIC

* Cake

* Quick

No three step quote tokens are supported. Each quote token must have a direct on-chain truth
for its dollar price. E.g. we cannot support pair FOOBAR:AAVE where AAVE would be converted
AAVE -> ETH -> USD.

Risk mitigations
----------------

Every price quote and price chart from Trading Strategy comes with the information of US dollar
exchange rate used for the reference price conversion for the quote token. You can always undo
the reference price conversion by multiplying the quoted dollar price with the exchange rate
and you get the original quote token amount back.

Notes
-----

About price oracle security
~~~~~~~~~~~~~~~~~~~~~~~~~~~

`Read this post about spot price security when used in an oracle <https://ethereum.stackexchange.com/a/114990/620>`_.