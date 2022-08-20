# Current

- Fix: `PairGroupedUniverse` did not have OHLCV data sorted by `timestamp`, but instead of was
  `pair_id`, `timestamp` causing some failures of managing expectations when accessing data.
- Fix: Update tests to reflect new datasets from Trading Strategy API
- Added `Client.fetch_candles_by_pair_ids` that loads trading data for certain pairs  
  data over JSONL endpoint  and avoids loading large Parquest candle and liquidity files.
  This makes it possible to run backtests on low memory environments.
- Added `Client.fetch_candle_dataset` and `read_parquest(filters)`
  to filter Parquest files when loading candle datasets to optimise memory usage.
  (At the moment this path is not used outside the tests.)
- Change `ChainId` base class to `IntEnum` instead of `Enum` for better type hinting

# 0.7.1

- Added `is_candle_green()`, `is_candle_red()` helpers
- Added `TimeBucket.to_pandas_timedelta()` helper
- Added `upsample_candles()` helper
- Added `unknown` and `osmosis` blockchains
 
# 0.7

- Add `Token` presentation
- Add `get_single_pair_data` shortcut
- Add `PandasPairUniverse.get_by_symbols` shortcut
- Add `PandasPairUniverse.get_all_tokens` shortcut
- Add `get_exchange_by_id` shortcut
- Add `summarydata.as_duration` Pandas summary table cell formatter
- Fix `filter_for_quote_tokens` to be more strict (was checking either side of the pair, not quote token)
- API change `create_single_pair_universe` now to accept `pick_by_highest_vol`
- API change: Rename `Universe.time_frame` -> `Universe.time_bucket` to be consistent
- Make download progress bars more human friendly 

# 0.6.9

- Add `check_schema` option when creating PyArrow table exports for trading pairs
- Fix type hint for `token1_decimals`
- Better error message for `get_one_pair_from_pandas_universe`
- Make `token0_symbol`  and `token1_symbol` optional as not all tokens have a symbol
- 
# 0.6.8

- Patch the previous release to `token0_decimals` and `token1_decimals` instead of `base_token_decimals` to be more aligned with the other Uniswap pair data
- Cleaning up [API and API documentation](https://tradingstrategy.ai/docs/programming/index.html#api-documentation)

# 0.6.7

- Export `base_token_decimals` and `quote_token_decimals` in [trading pair datasets](https://tradingstrategy.ai/docs/programming/api/pair.html)

# 0.6.6

- New time points for the [time buckets](https://tradingstrategy.ai/docs/programming/api/timebucket.html) 

# 0.6.5

- Add `exchange_address` as a part of [trading pair datasets](https://tradingstrategy.ai/docs/programming/api/pair.html)
- Add `get_by_chain_and_factory()` to [decentralised exchange universe](https://tradingstrategy.ai/docs/programming/api/exchange.html)

# 0.6.4

- Add `buy_tax`, `transfer_tax`, `sell_tax` to [trading pair datasets](https://tradingstrategy.ai/docs/programming/api/pair.html)

# 0.6.3

- Fix download retries in live trading
- Add `stablecoin` module for upcoming stablecoin support fuctions
- Add `DEXPair.convert_to_dataframe` 
- Add `filter_for_stablecoins` 
  

# 0.6.2

- Fix broken build

# 0.6.1

- Fix Macbook M1, macOS, arm64 compatibility. Updated Scipy, Pyarrow and Numpy dependencies.

# 0.6

- Renaming the package `tradingstrategy` -> `trading-strategy` to be consistent with the package naming practices.
  [See the new Github repository](https://github.com/tradingstrategy-ai/trading-strategy).

# 0.5.3

- Fix download responses being buffered in CLI
- Test for `get_trading_pair_page_url()` in the downloaded dataset
- Added support for Ganacher tester chain

# 0.5.2

- Added `Client.create_live_client` for live trading
- More helper functions to help unit testing
- `tradingstrategy.utils.time` to check for incompatible `pandas.Timestamp` formats
- Added `tradingstrategy.pair.filter_for_quote_tokens`
- Added `get_all_samples_by_range`, `iterate_samples_by_pair_range` shortcuts to consume trading pair data in strategies
- Added `get_prior_timestamp` to calibrate your clock with the existing time index
- Added `Client.clear_caches` to ensure fresh data downloads
- Added `DEXPair.exchange_slug` and `DEXPair.pair_slug` so we can point to web page URLs
- Renamed `Sushiswap` -> `Sushi` as per their branding

# 0.5.1

- Adding chain id 61 for Ethereum Classic / [Ethereum Tester](https://github.com/ethereum/eth-tester/)
- Adding `GroupedCandleUniverse.create_empty()` and `GroupedLiquidityUniverse.create_empty()`

# 0.5.0

This is a release to get Trading Strategy client towards live trading.

- Created `tradingstrategy.universe.Universe` helper class to encapsulate trading universe
- Added convenience method `PairUniverse.create_from_pyarrow_table_with_filters` so creating different trading universes is less lines of code
- Added convenience method `tradingstrategy.utils.groupeduniverse.filter_for_pairs_on_exchanges`
- Added helper methods to construct single trading pair universe, see `PandasPairUniverse.create_single_pair_universe`
- Separate QSTrader to an optional dependency. Please install as `pip install tradingstrategy[qstrader]`.
  This is because QSTrader depends on Seaborn that depends on SciPy that does not work so good on Apple M1 macs.
- Add the default connection and read timeout 15 seconds to all client downloads
 
# 0.4.0

- Reworked how QSTrader framework integration works. QSTrader was originally designed for stock markets, so it required few changes to make it more suitable for 24/7 cryptomarkets. **Warning**: QSTrader and its integration are both in beta. More work is needed to be done e.g. in fee calculations to make the integration smooth. See example on [PancakeSwap momentum trading algorithm](https://tradingstrategy.ai/docs/programming/algorithms/pancakeswap-momentum-naive.html).
- Added [portfolioanalyzer](https://tradingstrategy.ai/docs/programming/api/portfolioanalyzer.html) to make timeline of portfolio construction over the time. See example on [PancakeSwap momentum trading algorithm](https://tradingstrategy.ai/docs/programming/algorithms/pancakeswap-momentum-naive.html).
- Include `seaborn` as a dependency as it is required for `qstrader` plot output, otherwise `qstrader` was crashing.
- `PairUniverse` supports indexed lookups to deal with the high pair count.
- More convenience methods for candle and liquidity manipulation. 


# 0.3.4

- Adding the preliminaty price impact analysis. [See the example notebook](https://tradingstrategy.ai/docs/programming/examples/price-impact.html). [Read the blog post about the liquidity formation](https://tradingstrategy.ai/blog/announcing-support-for-liquidity-charts).

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