from abc import ABC, abstractmethod

import pandas as pd
import yfinance as yf
import duckdb

from src.utils.constants import STOCKS_RAW_TABLE_NAME
from src.utils.stock_duck_db_conn import StockDuckDbConn


class StockIngestor(ABC):
    ### ABSTRACT PROPERTIES AND METHODS ###
    @property
    @abstractmethod
    def ticker(self) -> str:
        pass

    ### PUBLIC METHODS ###

    def ingest_stock_data(self, start_date: str, end_date: str, interval: str = "15m"):
        df = self._download_stock_data(start_date, end_date, interval)

        with self._get_stock_db_data_conn() as conn:
            self._insert_stock_data(conn, df)
        
        return True
    
    ### PRIVATE METHODS ###

    def _download_stock_data(self, start_date: str, end_date: str, interval: str):
        df = yf.download(self.ticker, start=start_date, end=end_date, interval=interval)
        df.reset_index(inplace=True)
        df["ingestion_time"] = pd.Timestamp.now(tz="UTC")
        return df

    def _insert_stock_data(self, duckdb_conn: duckdb.DuckDBPyConnection, downloaded_stock_data_df: pd.DataFrame):
        duckdb_conn.execute(f"INSERT INTO {STOCKS_RAW_TABLE_NAME} SELECT * FROM downloaded_stock_data_df")

    def _get_stock_db_data_conn(self) -> duckdb.DuckDBPyConnection:
        return StockDuckDbConn().get_current_conn()