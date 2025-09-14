"""Backtesting with Lumibot.

inputs:
    - txt file with tickers
    - date of portfolio positions opening
"""

from pathlib import Path
from datetime import datetime

from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting
from lumibot.strategies import Strategy
from lumibot.traders import Trader


backtest_portfolio_tickers_file = r'sample_data\sample_backtest_portfolio_2023_09_01.txt'
backtest_date = '2024-09-01'
backtest_period = '2y'
tickers_list = Path(backtest_portfolio_tickers_file).read_text(encoding='utf-8').split('\n')

# get OHLC data from backtest_date to the period for all the tickers
# and load into a pandas df https://lumibot.lumiwealth.com/backtesting.pandas.html

# or see if lumibot is able to retrieve OHLC itself with yahoo finance (Polygon does not support TSE)
# https://lumibot.lumiwealth.com/backtesting.yahoo.html



# A simple strategy that buys AAPL on the first day
class MyStrategy(Strategy):
    def on_trading_iteration(self):
        if self.first_iteration:
            aapl_price = self.get_last_price('AAPL')
            quantity = self.portfolio_value // aapl_price
            order = self.create_order('AAPL', quantity, 'buy')
            self.submit_order(order)


# Pick the dates that you want to start and end your backtest in code
backtesting_start = datetime(2025, 1, 1)
backtesting_end = datetime(2025, 1, 31)

# Run the backtest
result = MyStrategy.run_backtest(
    YahooDataBacktesting,
    backtesting_start,
    backtesting_end,
)


