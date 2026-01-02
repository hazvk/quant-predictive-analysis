import pandas as pd
import yfinance as yf
import duckdb

from src.utils.logger import Logger
from src.utils.common.constants import STOCKS_RAW_TABLE_NAME
from src.utils.common.stock_duck_db_conn import StockDuckDbConn


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

    def ingest_stock_data(self, interval: str = "1d", start_date_override: str = None, end_date_override: str = None):
        self._logger.info(f"Starting ingestion for ticker: {self.ticker} from {start_date_override} to {end_date_override} with interval {interval}")
        
        df = self._download_stock_data(interval, start_date_override, end_date_override)

        with self._get_stock_db_data_conn() as conn:
            self._insert_stock_data(conn, df)
        
        self._logger.info(f"- Done")
        return True
    
    ### PRIVATE METHODS ###

    def _download_stock_data(self, interval: str, start_date_override: str, end_date_override: str) -> pd.DataFrame:
        start_date = start_date_override
        end_date = end_date_override or pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d")
        is_max_date_found = False
        
        with StockDuckDbConn().get_current_conn() as conn:
            max_date_found_df = conn.sql(f"""
                SELECT MAX(Date) as max_date FROM {STOCKS_RAW_TABLE_NAME}
                WHERE ticker = ?
                GROUP BY ticker
            """, params=[self.ticker]).to_df()

            if max_date_found_df.shape[0] > 0:
                self._logger.info(f"  - Existing data found for ticker {self.ticker}")
                is_max_date_found = True
                max_date = max_date_found_df["max_date"].iloc[0]
                if start_date is None:
                    start_date = (pd.to_datetime(max_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                    self._logger.info(f"  - Adjusted start_date to {start_date} based on existing data")
            else:
                self._logger.info(f"  - No existing data found for ticker {self.ticker}")
                is_max_date_found = False
            
        if is_max_date_found:
            df = yf.download(self.ticker, start=start_date, end=end_date, interval=interval)
            
            # self._logger.debug(df.columns)

            df.columns = df.columns.get_level_values(0) # keep only "Price" index
            df["Ticker"] = self.ticker

            df.reset_index(inplace=True)
            df["Interval_Type"] = interval
        else:
            df = yf.Ticker(self.ticker).history(period="max")
            df.reset_index(inplace=True)
            df["Ticker"] = self.ticker
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