# 0.3.3

- Make fastquant dependency optional, as it was causing installation issues with its ccxt dependency

# 0.3.2

- Updated inline interactive setup from within Jupyter notebook

# 0.3.1

- Polish README, release process and docs

# 0.3.0

- Multiple changes to make the multichain backtesting possible.

- [The documentation code examples](https://tradingstrategy.ai/docs/programming/index.html#code-examples)
  will be updated to reflect multichain support over time and may work incorrectly
  at the moment. [Getting Started](https://tradingstrategy.ai/docs/programming/examples/getting-started.html) 
  tutorial is already updated. 

- This is a major release deprecating activity flags in the trading pair universe.
  The multichain trading pair data is too big to include inactivate trading pairs (800k+ total trading pairs).
  Thus, the pair universe set only contains [active trading pairs](https://tradingstrategy.ai/docs/programming/tracking.html) after this release, 
  making the trading pair universe less than 100k trading pairs again, making it more feasible to download the data.

- `ExchangeUniverse.get_by_chain_and_slug()` is now canonical way to refer to an exchange
 
- `PairUniverse.get_pair_by_ticker_by_exchange()` is now canonical way to refer to a trading pair

- Clarify primary keys may not be stable and should no longer referred permanently 

- `Exchange.homepage` and `Exchange.active_pair_count` fields added
  
# 0.2.14

- Make `ChainId` database more deterministic by loading all supported chains once and only once

# 0.2.13

- Add `chain_name` output field for exchange data
 
# 0.2.12

- Adding slug lookups for chains 

# 0.2.11

- Add Binance Smart Chain and Polygon logos

# 0.2.10

- Fix the default API endpoint URL

# 0.2.9

- Adding embedded chain list repository
- Adding support for Binance Smart Chain

# 0.2.8

- Adding more metadata information on `ChainId` objects

# 0.2.7

- Adding `exchange_slug` and `chain_slug` on the `Exchange` object

# 0.2.6

- Allow to customise color range for the trade success distribution diagram

# 0.2.5

- Added custom `DEXTrader` base class, as the default logic with Backtrader was a bit insufficient 

- Added trade analysis support for Backtrader strategies (earlier was QSTrader only)

- Reworked trade analysis summaries and views 

- Added [advanced example for Double 7 trading strategy](https://tradingstrategy.ai/docs/programming/algorithms/double-7-advanced.html)

# 0.2.4

- Polish API key welcome setup

# 0.2.3

- Fixed not reading settings file from Google Drive