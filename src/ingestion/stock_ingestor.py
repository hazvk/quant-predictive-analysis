import pandas as pd
import yfinance as yf
import duckdb

from src.utils.logger import Logger
from src.utils.constants import STOCKS_RAW_TABLE_NAME
from src.utils.stock_duck_db_conn import StockDuckDbConn


class StockIngestor():
        
    _logger: Logger = Logger()
    _ticker: str = None

    def __init__(self, ticker: str):
        super().__init__()
        self._ticker = ticker

    ### PUBLIC METHODS ###

    @property
    def ticker(self) -> str:
        return self._ticker

    def ingest_stock_data(self, start_date: str, end_date: str, interval: str = "1d"):
        self._logger.info(f"Starting ingestion for ticker: {self.ticker} from {start_date} to {end_date} with interval {interval}")
        
        df = self._download_stock_data(start_date, end_date, interval)

        with self._get_stock_db_data_conn() as conn:
            self._insert_stock_data(conn, df)
        
        return True
    
    ### PRIVATE METHODS ###

    def _download_stock_data(self, start_date: str, end_date: str, interval: str):
        df = yf.download(self.ticker, start=start_date, end=end_date, interval=interval)
        
        # self._logger.debug(df.columns)

        df.columns = df.columns.get_level_values(0) # keep only "Price" index
        df["Ticker"] = self.ticker

        df.reset_index(inplace=True)
        df["Interval_Type"] = interval
        df["Ingestion_Time"] = pd.Timestamp.now(tz="UTC")

        # re-order columns for ingestion, doesn't filter out any columns
        df = df[["Ticker", "Interval_Type", "Date", "Open", "High", "Low", "Close", "Volume", "Ingestion_Time"]]

        # self._logger.debug(df.columns)

        return df

    def _insert_stock_data(self, duckdb_conn: duckdb.DuckDBPyConnection, downloaded_stock_data_df: pd.DataFrame):
        # self._logger.debug(downloaded_stock_data_df)
        # self._logger.debug(downloaded_stock_data_df.columns)

        duckdb_conn.execute(f"""
            INSERT INTO {STOCKS_RAW_TABLE_NAME} (
                SELECT *
                    , TRUE as is_latest_load
                FROM downloaded_stock_data_df
            );
        """)

    def _get_stock_db_data_conn(self) -> duckdb.DuckDBPyConnection:
        return StockDuckDbConn().get_current_conn()