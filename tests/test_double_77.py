"""Test code for double-77 trading strategy on ETH-USDC pair."""

import pytest
import pandas as pd

import backtrader as bt
from backtrader import analyzers
from backtrader import indicators

from tradingstrategy.chain import ChainId
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.client import Client
from tradingstrategy.frameworks.backtrader import prepare_candles_for_backtrader, add_dataframes_as_feeds
from tradingstrategy.pair import PandasPairUniverse

# Which pair we analyse
TARGET_PAIR = ("WETH", "USDC")  # https://analytics.sushi.com/pairs/0x795065dcc9f64b5614c407a6efdc400da6221fb0

# Use daily candles for backtesting
CANDLE_KIND = TimeBucket.d1

# The moving average must be above of this number for us to buy
MOVING_AVERAGE_CANDLES = 50

# How many previous candles we sample for the low close value
LOW_CANDLES = 7

# How many previous candles we sample for the high close value
HIGH_CANDLES = 7

# When do we start the backtesting - limit the candle set from the data dump from the server
BACKTESTING_BEGINS = pd.Timestamp("2020-10-1")

# When do we end backtesting
BACKTESTING_ENDS = pd.Timestamp("2021-09-1")

# Here is original PineScript for the comparison
ORIGINAL_PINE_SCRIPT = """
//@version=4
strategy("Double 7's Strat.v1", overlay=true, default_qty_type = strategy.percent_of_equity, default_qty_value = 100)

value1=input(7, title="Quantity of day low")
value2=input(7, title="Quantity of day high")
entry=lowest(close[1],value1)
exit=highest(close[1],value2)

mma100=sma(close,100)

plot(entry, title = '7 day low', color = color.blue,   linewidth = 1, style = plot.style_linebr)  // plot 7 day low
plot(exit, title = '7 day high', color = color.purple,   linewidth = 1, style = plot.style_linebr)  // plot 7 day high
plot(mma100, title = 'MMA100', color = color.orange,   linewidth = 2, style = plot.style_line)  // plot MMA100

//Stop Loss input setting
longLossPerc = input(title="Long Stop Loss (%)", type=input.float, minval=0.0, step=0.1, defval=1) * 0.01

// Determine stop loss price
longStopPrice  = strategy.position_avg_price * (1 - longLossPerc)

// Plot stop loss values for confirmation
plot(series=(strategy.position_size > 0) ? longStopPrice : na, color=color.red, style=plot.style_linebr, linewidth=2, title="Long Stop Loss")

// Test Period
testStartYear = input(2021, "Backtest Start Year")
testStartMonth = input(5, "Backtest Start Month")
testStartDay = input(17, "Backtest Start Day")
testPeriodStart = timestamp(testStartYear,testStartMonth,testStartDay,0,0)

testStopYear = input(2022, "Backtest Stop Year")
testStopMonth = input(12, "Backtest Stop Month")
testStopDay = input(30, "Backtest Stop Day")
testPeriodStop = timestamp(testStopYear,testStopMonth,testStopDay,0,0)

testPeriod() =>
    time >= testPeriodStart and time <= testPeriodStop ? true : false

if testPeriod()
    if (close>mma100) and (close<entry)
        strategy.entry("RsiLE", strategy.long , comment="Open")

    if (close>exit)
        strategy.close_all()

// Submit exit orders based on calculated stop loss price
if (strategy.position_size > 0)
    strategy.exit(id="XL STOP", stop=longStopPrice)
"""


class Double77(bt.Strategy):
    """An example of double-77 strategy for DEX spot trading.

    The original description: https://www.thechartist.com.au/double-7-s-strategy/
    """

    def start(self):
        # Set up indicators used in this strategy

        # Moving v
        self.moving_average = indicators.SMA(period=MOVING_AVERAGE_CANDLES)

        # The highest close price for the N candles
        # "exit" in pine script
        self.highest = indicators.Highest(self.data.close, period=HIGH_CANDLES)

        # The lowest close price for the N candles
        # "entry" in pine script
        self.lowest = indicators.Lowest(self.data.close, period=LOW_CANDLES)

        # Count ticks and some basic testing metrics
        self.ticks = self.enters = self.exits = 0

    def next(self):
        """Execute a decision making tick for each candle."""

        self.ticks += 1

        # Get the last value of the current candle close and indicators
        # More about self.data and self.lines of Backtrader
        # https://github.com/backtrader/backtrader-docs/blob/master/docs/concepts.rst#indexing-0-and--1
        # https://www.fatalerrors.org/a/backtrader-3-core-concept-lines.html.
        # Note that the value can be "nan" if we have not ticked enough days and
        # e.g. the moving average cannot be calculated.
        # Zero means "today" or the indicator value at the current candle.
        # Minus one means "yesterday" or the indicator value at the previous candle.
        # (Indexing might be different from the pine script)
        close = self.data.close[0]
        low = self.lowest[-1]
        high = self.highest[-1]
        avg = self.moving_average[0]

        print(f"Tick: {self.ticks}, close: {close}, avg: {avg}, low: {low}, high: {high}")

        if not all([close, low, high, avg]):
            # Do not try to make any decision if we have nan or zero data
            return

        # Enter when we are above moving average and the daily close was
        if close >= avg and close <= low and not self.position:
            print("Entered to a position")
            self.enters += 1
            self.buy()

        # If the price closes above its 7 day high, exit from the markets
        if close >= high and self.position:
            print("Exited the position")
            self.exits += 1
            self.close()


def test_double_77(logger, persistent_test_client: Client):
    """Mutliasset strategy runs correct number of days."""

    client = persistent_test_client
    
    # Operate on daily candles
    strategy_time_bucket = CANDLE_KIND

    exchange_universe = client.fetch_exchange_universe()
    columnar_pair_table = client.fetch_pair_universe()
    all_pairs_dataframe = columnar_pair_table.to_pandas()
    pair_universe = PandasPairUniverse(all_pairs_dataframe)

    # Filter down to pairs that only trade on Sushiswap
    sushi_swap = exchange_universe.get_by_name_and_chain(ChainId.ethereum, "sushiswap")
    pair = pair_universe.get_one_pair_from_pandas_universe(
        sushi_swap.exchange_id,
        TARGET_PAIR[0],
        TARGET_PAIR[1])

    all_candles = client.fetch_all_candles(strategy_time_bucket).to_pandas()
    pair_candles: pd.DataFrame = all_candles.loc[all_candles["pair_id"] == pair.pair_id]
    pair_candles = prepare_candles_for_backtrader(pair_candles)

    # We limit candles to a specific date range to make this notebook deterministic
    pair_candles = pair_candles[(pair_candles.index >= BACKTESTING_BEGINS) & (pair_candles.index <= BACKTESTING_ENDS)]

    print(f"Out candle universe size is {len(pair_candles)}")

    # This strategy requires data for 100 days. Because we are operating on new exchanges,
    # there simply might not be enough data there
    assert len(pair_candles) > MOVING_AVERAGE_CANDLES, "We do not have enough data to execute the strategy"

    # Create the Backtrader back testing engine "Cebebro"
    cerebro = bt.Cerebro(stdstats=False)

    # Add out strategy
    cerebro.addstrategy(Double77)

    # Add two analyzers for the strategy performance
    cerebro.addanalyzer(analyzers.Returns, _name="returns")
    cerebro.addanalyzer(analyzers.TradeAnalyzer, _name="tradeanalyzer")

    # Add our SUSHI-ETH price feed
    add_dataframes_as_feeds(
        cerebro,
        pair_universe,
        [pair_candles],
        BACKTESTING_BEGINS,
        BACKTESTING_ENDS,
        strategy_time_bucket)

    results = cerebro.run()

    strategy: Double77 = results[0]

    # We run the strategy over 202 days
    returns: analyzers.Returns = strategy.analyzers.returns
    assert returns.rets["rtot"] == pytest.approx(0.061144023898301814)

    # How many days the strategy run
    assert strategy.ticks == 336
    assert strategy.enters == 9
    assert strategy.exits == 9

    trade_analyzer: analyzers.TradeAnalyzer = strategy.analyzers.tradeanalyzer
    assert trade_analyzer.rets["won"]["total"] == 8
    assert trade_analyzer.rets["lost"]["total"] == 1

