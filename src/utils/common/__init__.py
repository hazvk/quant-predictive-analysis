import pandas as pd

from src.utils.config import STOCKS_TO_CALCULATE

STOCK_PORTFOLIOS_DF = pd.DataFrame.from_records([[t,tick] for t, tickers in STOCKS_TO_CALCULATE.items() for tick in tickers])