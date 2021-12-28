[![CI Status](https://github.com/tradingstrategy-ai/client/actions/workflows/python-app.yml/badge.svg)](https://github.com/tradingstrategy-ai/client/actions/workflows/python-app.yml)

[![pip installation works](https://github.com/tradingstrategy-ai/client/actions/workflows/pip-install.yml/badge.svg)](https://github.com/tradingstrategy-ai/client/actions/workflows/pip-install.yml)

[![Trading Strategy logo](https://hv4gxzchk24cqfezebn3ujjz6oy2kbtztv5vghn6kpbkjc3vg4rq.arweave.net/n8pMe2r9Wv3oQsPk4Swie55CZLgXWuExDsBOtczNdCY)](https://tradingstrategy.ai)

# Trading Strategy protocol client

Trading Strategy client is a Python library for on-chain algorithmic trading. 
It is using [backtesting data](https://tradingstrategy.ai/trading-view/backtesting) and [real-time price feeds](https://tradingstrategy.ai/trading-view)
from [Trading Strategy Protocol](https://tradingstrategy.ai/). 

# Use cases

* Analyse cryptocurrency investment opportunities on decentralised exchhanges (DEXes)

* Creating trading algorithms and trading bots that trade on DEXes

* Deploy investable trading strategies as on-chain smart contracts

# Features

* Getting trading data from on-chain decentralised exchanges like Uniswap and PancakeSwap

* Integration with [Jupyter Notebook](https://jupyter.org/) for easy manipulation of data 

* Utilise Python quant frameworks like [Backtrader](https://github.com/tradingstrategy-ai/backtrader) and [QSTrader](https://github.com/tradingstrategy-ai/qstrader) to create, analyse and backtest DEX trading algorithms 

# Example and getting started 

See [the Getting Started notebook](https://tradingstrategy.ai/docs/programming/examples/getting-started.html) and the rest of the [Trading Strategy documentation](https://tradingstrategy.ai/docs/).

# Prerequisites

Python 3.8+

# Installing the package

**Note**: Unless you are an experienced Python developer, [the suggested usage of Trading Algorithm framework is using Google Colab hosted environments](https://tradingstrategy.ai/docs/programming/examples/getting-started.html).

You can install this package with `pip` or `poetry`

```shell
pip install tradindstrategy 
```

```shell
poetry add tradindstrategy
```

# Documentation

[Read documentation online](https://tradingstrategy.ai/docs/).

# Development

This git repository contains submodules. Remember to do:

```shell
git submodule update --init --recursive  
```

# License

GNU AGPL 3.0. 
