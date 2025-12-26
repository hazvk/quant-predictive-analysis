import yfinance as yf
from abc import ABC, abstractmethod

class StockIngestor(ABC):

    @property
    @abstractmethod
    def ticker(self) -> str:
        pass

    def ingest_stock_data(self, start_date: str, end_date: str, interval: str = "15m"):
        df = yf.download(self.ticker, start=start_date, end=end_date, interval=interval)
        return df
        # TODO save using Duck