[![CI Status](https://github.com/tradingstrategy-ai/trading-strategy/actions/workflows/python-app.yml/badge.svg)](https://github.com/tradingstrategy-ai/trading-strategy/actions/workflows/python-app.yml)

[![pip installation works](https://github.com/tradingstrategy-ai/trading-strategy/actions/workflows/pip-install.yml/badge.svg)](https://github.com/tradingstrategy-ai/trading-strategy/actions/workflows/pip-install.yml)

<a href="https://tradingstrategy.ai">
  <img src="https://raw.githubusercontent.com/tradingstrategy-ai/trading-strategy/master/logo.svg" width="384">
</a>

# Trading Strategy framework for Python

Trading Strategy framework is a Python framework for algorithmic trading on decentralised exchanges. 
It is using [backtesting data](https://tradingstrategy.ai/trading-view/backtesting) and [real-time price feeds](https://tradingstrategy.ai/trading-view)
from [Trading Strategy Protocol](https://tradingstrategy.ai/). 

# Use cases

* Analyse cryptocurrency investment opportunities on [decentralised exchanges (DEXes)](https://tradingstrategy.ai/trading-view/exchanges)

* Creating trading algorithms and trading bots that trade on DEXes

* Deploy trading strategies as on-chain smart contracts where users can invest and withdraw with their wallets

# Features

* Supports multiple blockchains like [Ethereum mainnet](https://tradingstrategy.ai/trading-view/ethereum), 
  [Binance Smart Chain](https://tradingstrategy.ai/trading-view/binance) and 
  [Polygon](https://tradingstrategy.ai/trading-view/polygon)

* Access trading data from on-chain decentralised exchanges like
  [SushiSwap](https://tradingstrategy.ai/trading-view/ethereum/sushi), [QuickSwap](https://tradingstrategy.ai/trading-view/polygon/quickswap) and [PancakeSwap](https://tradingstrategy.ai/trading-view/binance/pancakeswap-v2)

* Integration with Jupyter Notebook for easy manipulation of data.
  See [example notebooks](https://tradingstrategy.ai/docs/programming/code-examples/index.html).

* Write [algorithmic trading strategies](https://tradingstrategy.ai/docs/programming/strategy-examples/index.html) for  decentralised exchange 

# Getting started 

See [the Getting Started tutorial](https://tradingstrategy.ai/docs/programming/code-examples/getting-started.html) and the rest of the [Trading Strategy documentation](https://tradingstrategy.ai/docs/).

# Prerequisites

* Python 3.10

# Installing the package

**Note**: Unless you are an experienced Python developer, [try the Binder cloud hosted Jupyter notebook examples first](https://tradingstrategy.ai/docs/programming/code-examples/index.html).

You can install this package with 

[Poetry](https://python-poetry.org/) as a dependency:

```shell
poetry add trading-strategy -E direct-feed
```

Poetry, local development:

```shell
poetry install -E direct-feed
```

Pip:

```shell
pip install "trading-strategy[direct-feed]" 
```

# Documentation

- [Read Trading Strategy documentation](https://tradingstrategy.ai/docs/).
- [Documentation Github repository](https://github.com/tradingstrategy-ai/docs).

Community
---------

* [Trading Strategy website](https://tradingstrategy.ai)

* [Blog](https://tradingstrategy.ai/blog)

* [Twitter](https://twitter.com/TradingProtocol)

* [Discord](https://tradingstrategy.ai/community#discord) 

* [Telegram channel](https://t.me/trading_protocol)

* [Changelog and version history](https://github.com/tradingstrategy-ai/trading-strategy/blob/master/CHANGELOG.md)

[Read more documentation how to develop this package](https://tradingstrategy.ai/docs/programming/development.html).

# License

GNU AGPL 3.0. 
