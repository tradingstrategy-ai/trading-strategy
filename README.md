[![PyPI version](https://badge.fury.io/py/trading-strategy.svg)](https://badge.fury.io/py/trading-strategy)

[![CI Status](https://github.com/tradingstrategy-ai/trading-strategy/actions/workflows/python-app.yml/badge.svg)](https://github.com/tradingstrategy-ai/trading-strategy/actions/workflows/python-app.yml)

[![pip installation works](https://github.com/tradingstrategy-ai/trading-strategy/actions/workflows/pip-install.yml/badge.svg)](https://github.com/tradingstrategy-ai/trading-strategy/actions/workflows/pip-install.yml)

<a href="https://tradingstrategy.ai">
  <img src="https://raw.githubusercontent.com/tradingstrategy-ai/trading-strategy/master/logo.svg" width="384">
</a>

# Trading Strategy framework for Python

Trading Strategy framework is a Python framework for algorithmic trading on decentralised exchanges. 

- Download decentralised finance market data sets
- Develop and backtest trading strategies in Jupyter Notebook
- Live trade execution for onchain trading
- Smart contract vault support for turning your trading strategy to a third-party investable vault

The `trading-strategy` library provides data fetching for backtesting and live trading.

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

See [the Getting Started repository](https://github.com/tradingstrategy-ai/getting-started) and the rest of the [Trading Strategy documentation](https://tradingstrategy.ai/docs/).

For JSON-only vault readiness polling and ETag-verified price snapshots, see
[Vault scan readiness receipts](tradingstrategy/vault_scan_manifest.py).

Historical HyperCore deposit permission uses original observation clocks,
separately from price/publication time. See
[`convert_vault_prices_to_vault_state()`](tradingstrategy/alternative_data/vault.py)
for explicit permission-history input and
[`select_permission_state()`](tradingstrategy/vault_permission.py) for snapshot
precedence and uncertainty handling. Recovered legacy flags use inferred price
clocks with visible provenance. Recorded leader shares and deposit caps remain
readable when older schemas have no separate capacity clock. A zero cap blocks
deposits without declaring the venue closed; a NULL cap remains unknown. Newer
coherent responses, including unknowns, replace the whole policy snapshot.

# Prerequisites

* Python 3.10

* [Understanding Python package management and installation](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/)
  (unless using Dev Container from teh above)

# Installing the package

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

**Note**: `trading-strategy` package provides trading data
download and management functionality only. If you want to [developed
automated trading strategies you need to install trade-executor package as well](https://github.com/tradingstrategy-ai/trade-executor/).

# Documentation

- [Documentation](https://tradingstrategy.ai/docs/).

- [Getting started](https://tradingstrategy.ai/docs/getting-started.html).

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

## Vault permission sidecars

The vault-state converter preserves explicit permission/capacity projections, including unknown values and original evidence clocks. For older sidecars without those projections it uses the shared eth-defi Hyperliquid classifier; it does not maintain another leader-share threshold or HLP-parent exception. Missing relationship metadata remains uncertain rather than being assumed to identify a normal vault.

Legacy sidecars without permission/capacity projections require
`eth_defi.hyperliquid.vault.classify_hyperliquid_vault_deposit`, introduced in
[eth-defi PR #1596](https://github.com/tradingstrategy-ai/web3-ethereum-defi/pull/1596).
PyPI release 1.2 does not contain this API. Development and CI therefore use the
Git source pinned in the development dependency group and lockfile (`44f365a`,
GitHub HEAD checked on 27 October 2026); `poetry install --all-extras` installs
that source, including the current Web3 7 dependency stack.

The library's version-based dependency allows a parent trade-executor checkout
to supply its editable `deps/web3-ethereum-defi` Git checkout. That is the normal
notebook/strategy setup; it must contain the shared classifier. The development
Git pin is not imposed on that parent project's dependency resolution. Update
source pins deliberately rather than using a moving branch in the lockfile.

Price-only conversion and sidecars with recorded permission/capacity fields do
not import or call the classifier. A missing explanatory reason string alone
does not trigger protocol reclassification. Strategy admission policy belongs
to the executor's `tradeexecutor.strategy.vault_closure` module.
