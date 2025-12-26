from src.utils.logger import Logger
import pandas as pd
import duckdb
import pandas_ta as pa

from src.utils.constants import STOCKS_CURATED_TABLE_NAME, STOCKS_RAW_TABLE_NAME
from src.utils.stock_duck_db_conn import StockDuckDbConn


class StockCurator():
        
    _logger = Logger()
    _ticker: str = None

    def __init__(self, ticker: str):
        super().__init__()
        self._ticker = ticker

    ### PUBLIC METHODS ###

    @property
    def ticker(self) -> str:
        return self._ticker

    def curate_stock_data(self):
        self._logger.info(f"Starting curation for ticker: {self.ticker}")

        self._logger.info("Processing data only for latest load of data")        
        load_df_with_ta = self._get_latest_load_df()

        if len(load_df_with_ta) == 0:
            self._logger.warning(f"No data found for ticker: {self.ticker} to curate.")
            return False

        load_df_with_ta["RSI"] = pa.rsi(load_df_with_ta.close, length=16)
        load_df_with_ta["CCI"] = pa.cci(load_df_with_ta.high, load_df_with_ta.low, load_df_with_ta.close, length=16)
        load_df_with_ta["AO"] = pa.ao(load_df_with_ta.high, load_df_with_ta.low)
        load_df_with_ta["MOM"] = pa.mom(load_df_with_ta.close, length=16)
        a = pa.macd(load_df_with_ta.close)
        load_df_with_ta = load_df_with_ta.join(a)
        load_df_with_ta["ATR"] = pa.atr(load_df_with_ta.high, load_df_with_ta.low, load_df_with_ta.close, length=16)
        load_df_with_ta["BOP"] = pa.bop(load_df_with_ta.open, load_df_with_ta.high, load_df_with_ta.low, load_df_with_ta.close, length=16)
        load_df_with_ta["RVI"] = pa.rvi(load_df_with_ta.close)
        a = pa.dm(load_df_with_ta.high, load_df_with_ta.low, length=16)
        load_df_with_ta = load_df_with_ta.join(a)
        a = pa.stoch(load_df_with_ta.high, load_df_with_ta.low, load_df_with_ta.close)
        load_df_with_ta = load_df_with_ta.join(a)
        a = pa.stochrsi(load_df_with_ta.close, length=16)
        load_df_with_ta = load_df_with_ta.join(a)
        load_df_with_ta["WPR"] = pa.willr(load_df_with_ta.high, load_df_with_ta.low, load_df_with_ta.close, length=16)

        load_df_with_ta["load_time"] = pd.Timestamp.now(tz="UTC")

        with self._get_stock_db_data_conn() as conn:
            conn.execute(f"""
                BEGIN TRANSACTION;
                
                MERGE INTO {STOCKS_CURATED_TABLE_NAME}
                    USING load_df_with_ta
                    ON {STOCKS_CURATED_TABLE_NAME}.ticker = load_df_with_ta.ticker
                        AND {STOCKS_CURATED_TABLE_NAME}.date = load_df_with_ta.date
                    WHEN MATCHED THEN UPDATE
                    WHEN NOT MATCHED THEN INSERT;

                UPDATE {STOCKS_RAW_TABLE_NAME} SET is_latest_load = FALSE WHERE ticker = '{self.ticker}';

                COMMIT;
                """)
        return True
    
    ### PRIVATE METHODS ###

    def _get_stock_db_data_conn(self) -> duckdb.DuckDBPyConnection:
        return StockDuckDbConn().get_current_conn()
    
    def _get_latest_load_df(self) -> pd.DataFrame:
        with self._get_stock_db_data_conn() as conn:
            return conn.sql(f"""
                SELECT 
                    raw.ticker
                    , raw.date
                    , raw.open
                    , raw.high
                    , raw.low
                    , raw.close
                    , raw.volume
                FROM {STOCKS_RAW_TABLE_NAME} AS raw
                WHERE raw.ticker = '{self.ticker}'
                    AND is_latest_load = TRUE
                ORDER BY raw.date ASC
                """).to_df()