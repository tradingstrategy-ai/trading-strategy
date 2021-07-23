import backtrader as bt


class TestStrategy(bt.Strategy):
    """A strategy that just logs closing prices.

    See https://www.backtrader.com/docu/quickstart/quickstart/
    """

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close

    def next(self):
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.dataclose[0])

        if self.dataclose[0] < self.dataclose[-1]:
            # current close less than previous close

            if self.dataclose[-1] < self.dataclose[-2]:
                # previous close less than the previous close

                # BUY, BUY, BUY!!! (with all possible default parameters)
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                self.buy()

import datetime

import backtrader.feeds as btfeeds

# Backtrader identifies timestamp column by index
sushi_candles = sushi_candles.set_index(pd.DatetimeIndex(sushi_candles["timestamp"]))
sushi_candles["volume"] = sushi_candles["buy_volume"] + sushi_candles["sell_volume"]

sushi_candles = sushi_candles.loc[sushi_candles['pair_id'] == 7531]

print(f"Feeding {len(sushi_candles)} to Celebro")

# Create a cerebro entity
cerebro = bt.Cerebro(stdstats=False)

# Add a strategy
cerebro.addstrategy(TestStrategy)

# Get a pandas dataframe

# Pass it to the backtrader datafeed and add it to the cerebro

data = bt.feeds.PandasData(dataname=sushi_candles)
cerebro.adddata(data)

start = datetime.datetime(2020, 9, 1)
end = datetime.datetime(2021, 4, 1)
