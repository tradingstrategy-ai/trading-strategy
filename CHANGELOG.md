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