# Current

- Optimisation: `get_price_with_tolerance()`: 
  candle price lookup is 40x faster for exact timestamp matches

# 0.16

- API change: Now `get_single_pair_data()` raises `NoDataAvailable` instead
  of returning an empty data frame by default if there is not enough data at source 
- Fix unnecessary upper() in `is_stablecoin_like`
- Fix Anvil chain id not fitting to Parquet data frame (assumed 16 bit uint for chain id)

# 0.15.2

- Add more stablecoins
- Add `DEXPair.volume_30d` shortcut
- Add `DEXPair.from_row` shortcut

# 0.15.1

- Fix: Better error messages when `fetch_trading_data_availability()` cannot process input

# 0.15

- Add `DEXPair.fee_tier` to get the trading pair fee as 0..1 float
- Add `PandasPairUniverse.create_pair_universe`
- Deprecate `PandasPairUniverse.create_single_pair_universe`
- Deprecate `PandasPairUniverse.create_limited_pair_universe`
- Add `USDT.e` and `USDC.e` bridged stablecoins on Avalanche

# 0.14.1

- Add: caching to `get_candles_by_pair()` and `get_single()` methods.
This can result in backtesting time being more than halved. 
- Fix: Bad wick filtering code crashed on empty dataframes

# 0.14

- Add: `tradingstrategy.utils.groupeduniverse.fix_bad_wicks` to deal with candle data where high and low
  values are abnormal due to various price manipulation issues. 
- Update: Automatically filter bad wicks when creating a candle universe
- Add a shortcut function `tradingstrategy.pair.generate_address_columns()` to generate `base_token_address` and `quote_token_address` columns
- Update API: `resolve_pairs_based_on_ticker()` supports `HumanReadableTradingPairDescription`
- Fix: Some error message polish

# 0.13.18

- Have classical `Candle.volume` field as buy volume and sell volume cannot be separated for Uniswap v3

# 0.13.17

- change `resample_candles()` to use timedelta instead of timebucket

# 0.13.16

- Fix JSON data loading for Uni v3 exchanges that lack trade count

# 0.13.15

- Add `fee_tier` option to `get_one_pair_from_pandas_universe()` and `get_pair_by_human_description()`

# 0.13.14

- Bump max pairs to load over JSONL to 75

# 0.13.13

- Chart visualization fixes
  - remove `price_chart_rel_size` and `subplot_rel_size` options (will all be specified in `relative_sizing`)
  - refactor and clean code in `tradingstrategy/charting/candle_chart.py`

# 0.13.12

- Fix SyntaxError caused by a typo

# 0.13.11

- We have now `tradingstrategey.client.BaseClient` as the base class for different client implementations
- Adding `UniswapV2MockClient` implementation that mimics `Client`, but reads all data
  from an EVM test backend. Makes live trading tests much easier.
- Small improvements to PairUniverse

# 0.13.10

- Fix detached indicator charting bugs for different `volume_bar_modes`
- Adds support for different subtitle font sizes
- Adds tests to make to catch charting bugs earlier in the future

# 0.13.9

- Bump depenedncies
- Add `ChainId.anvil`
- Add support for detached technical indicators
- Web3 6.0 fixes

# 0.13.8

- Match dependencies and Python version pindowns with Web3.py 6.0
- Adding [Ethereum Tester](https://github.com/ethereum/eth-tester/) chain id 

# 0.13.7

- Silence pandas warning in `PairGroupedUniverse.get_pair_ids()`
- Rename "Binance Smart Chain" to "BNB Smart Chain"

# 0.13.6

- Add a new method `Client.fetch_all_lending_protocol_reserves()` for fetching data on
  supported decentralized lending protocols (only AAVE v3 at present).
- Improve `validate_ohclv_dataframe()` to check for date/timestamp index as well as column

# 0.13.5

- Fixes for filtering by `chain_id` in `ExchangeUniverse`

# 0.13.4

- Improve `resolve_pairs_based_on_ticker()` to be able to filter pairs with specified fee. Change 
default sorting criteria `buy_volume_all_time` to be function argument for more flexible filtering

# 0.13.3

- Add `GroupedCandleUniverse.get_last_entries_by_pair_and_timestamp(pair_id, timestamp)` for simplified 
  candle data access
- Add `PandasPairUniverse.get_pair(chain_id, exchange_id, base_token, quote_token)` for simplified 
  pair access
- Support passing `exchange_universe` in `PandasPairUniverse` construction, so we have all metadata
  needed to look up pairs
- Fixes to various token data accessor

# 0.13.2
- 
- Same as 0.13.1 - fixing broken release

# 0.13.1

- Support Arbitrum L2 in `ChainId` class

# 0.13.1

A large impact optimisation update.

- Optimise `PandasPairUniver.get_pair_by_id` to cache full `DEXPair` objects to speed up
  strategies using large number of trading pairs

- Make `DEXPair` Python `__slots__` based to make the object property access faster

- Added `ResampledLiquidityUniverse` for faster backtesting with luidity data

- Allow different granularity of OHLCV and liquidity data in `Universe`

# 0.13

- Release failed

# 0.12.5
- 
- Add `LiquidityUniverse.get_liquidity_with_tolerance`

# 0.12.4
- 
- Quick fix on `get_pair_by_human_description` error message

# 0.12.3

- Improve `CandleSampleUnavailable` error messge further to offer friendly advises how to fix
- Improve `ExchangeUniverse` interface with more helpful shorthand methods

# 0.12.2

- Add `ExchangeUniverse.from_collection()`
- Pass `ExchangeUniverse` instance as a part of `Universe.exchange_universe` instead just raw array of
  of `univer.exchanges`

# 0.12.1

- Better multipair lookup when typing out pair names by hand 
- Add `tradingstrategy.pair.HumanReadableTradingPairDescription`
- Add `PandasPairUniverse.get_pair_by_human_description`
- 
# 0.12

- Add default HTTP request retry policy to `Client`
- Add default HTTP user agent to `Client`
- More direct data feed infrastructure
- Fix `create_requests_client(add_exception_hook=True)` not working correctly
- Add `GroupedCandleUniverse.create_from_multiple_candle_datafarames`

# 0.11.1

- Add: Make `DEXPair` hashable
- Fix: Historical candle download for JSONL endpoint - make time ranges to work correctly 
- Change: Make JSONL cache filenames more descriptive 

# 0.11

- Adding `Client.fetch_trading_data_availability` API
- Moved `types.py` to properly Python 3.10 TypeAlias system

# 0.10.2 

- Add `TradeSummary.show` to simplify usage in notebooks

# 0.10.1

- Fix missing dependencies because web3-ethereum-defi data extras
- Fix `make_candle_labels` not working correctly if some keys were not present in the OHCLV DataFrame
- Add `VolumeBarMode` option to `visualise_ohlcv` to allow different rendering modes for volume bars, in-chart 
  or outside chart
- More implementation of direct-to-blockchain node data feeds

# 0.10.0

- Add the initial implementation for direct-to-blockchain node data feeds

# 0.9.1

- Add support for average duration statistic to be expressed in terms of `bars`
- Framework for direct, real-time, date feeds directly from a blockchain node
- Add `uniswap_v2_incompatible` exchange type

# 0.9.0

- Make the default behavior not to return the current candle if asked by timestamp

# 0.8.8

- Switch to `tqdm-loggable` to have better download status behavior on headless configurations
- Fine tune `visualise_ohlcv` label configuration

# 0.8.7

- Fix spelling `visualise_ohlcv`

# 0.8.6

- Add candle charts with `visualise_ohclv`

# 0.8.5

- Optimize `groupeduniverse` class, specifically `get_timestamp_range()` method

- Optimize get pair speed

- Remove the "no Content-Length header" warning if HTTP response lacks file size information

- Fix `get_token()` not returning symbol information

# 0.8.4

- Added `Client.close()` and `CachedHTTPTransport.close()` for explicit closing of streams
- Fix `JUPYTER_PLATFORM_DIRS` warning when importing
- Fixed bunch of Pandas warnings
- Add `pytest -Werror` to ensure the lib does not raise any warnings

# 0.8.3

- Escape hatch for interactive API key setup loop
- Added `Candle.generate_synthetic_sample` for mocking test data
- Added `GroupedCandleUniverse.get_price_with_tolerance` to safely get the latest price
  in backtesting

# 0.8.2

- Make `matplotlib` optional in `Client.setup_notebook`
- Added `DEXPair.exchange_type` to support Uniswap v3 pairs

# 0.8.1

- Change: List `requests` as an explicit dependency for the client
  (Might have been causing issues with Pyodide builds)

# 0.8.0

- Change: Remove a lot of dependencies and cleaning up old code.
  The package with its dependencies should be now much more compact.
  Any old Backtrader or QSTrader code is behind an extra dependency in 
  the Python package definition. There is no longer dependency to 
  Ethereum or crypto packages that had C and were problematic
  install. 
- Change: preparing for Pyodide support.
- Change: Bumped Python 3.10 because 3.10 is minimum for Pyodide

# 0.7.2

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
- Change `PandasPairUniverse.build_index` to use Pandas DataFrame frame transpose,
  greatly speeding up the creation of the index
- Fix: `get_closest_price` gets a closest price even if the timestamp and  
  and time bucket are not in the same internal 
- Added `environment/jupyterlite` for the upcoming WebAssembly integration

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