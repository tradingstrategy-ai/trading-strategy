![CI status](https://github.com/tradingstrategy-ai/client/badge.svg)

# Trading Strategy protocol client

Trading Strategy client is a Python library for on-chain algorithmic trading and trading bots. 

# Use cases

* Analysing of cryptocurrency investment opportunities

* Creating trading algorithms and bots that operate on on-chain data

# Features

* Getting trading data from on-chain decentralised exchanges like Uniswap and PancakeSwap

* Integration with [Jupyter Notebook](https://jupyter.org/) for easy manipulation of data 

* Use Python quant toolkits like [Zipline](https://github.com/stefan-jansen/zipline-reloaded) to create, analyse and backtest algorithms

* (*soon*) Decentralised execution of trading algorithms through smart contracts

# Example and getting started 

See [the Getting Started notebook](https://tradingstrategy.ai/docs/programming/examples/getting-started.html) and the rest of the [Trading Strategy documentation](https://tradingstrategy.ai/docs/).

## Prerequisites

Python 3.7+

## Adding the package

Install with `pip` or `poetry`

```shell
pip install tradindstrategy 
```

```shell
poetry add tradindstrategy
```

# Oh-chain trading data

Currently trading data is downloaded from capitalgram.com candle server. 
The access to the data is free, but you need to register to Capitalgram mailing list to get an API key.  

# License

GNU AGPL 3.0. 
