import argparse
import os
from pathlib import Path
from dotenv import load_dotenv
import duckdb

from src.utils.logger import Logger
from src.utils.common.constants import STOCKS_CURATED_TABLE_NAME, STOCKS_RAW_TABLE_NAME
from src.utils.common.stock_duck_db_conn import StockDuckDbConn

_logger = Logger()

def _create_data_directory():
    ingestion_path = os.environ["QUANT_PRED_ANALYSIS_DUCKDB_STORAGE_PATH"]
    try:
        Path(ingestion_path).mkdir(parents=True)
        _logger.warning(f"Created data directory {ingestion_path}")
    except FileExistsError:
        _logger.warning(f"Data directory {ingestion_path} already exists")

def _create_tables(duckdb_conn: duckdb.DuckDBPyConnection):
    duckdb_conn.execute(f"""
        CREATE TABLE {STOCKS_RAW_TABLE_NAME} (
            ticker STRING,
            interval_Type STRING,
            date TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            ingestion_time TIMESTAMP,
            is_latest_load BOOLEAN DEFAULT TRUE
        )
    """)

    _logger.warning(f"Table {STOCKS_RAW_TABLE_NAME} created in DuckDB")

    duckdb_conn.execute(f"""
        CREATE TABLE {STOCKS_CURATED_TABLE_NAME} (
            ticker VARCHAR,
            date TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            RSI DOUBLE,
            CCI DOUBLE,
            AO DOUBLE,
            MOM DOUBLE,
            MACD_12_26_9 DOUBLE,
            MACDh_12_26_9 DOUBLE,
            MACDs_12_26_9 DOUBLE,
            ATR DOUBLE,
            BOP DOUBLE,
            RVI DOUBLE,
            DMP_16 DOUBLE,
            DMN_16 DOUBLE,
            STOCHk_14_3_3 DOUBLE,
            STOCHd_14_3_3 DOUBLE,
            STOCHh_14_3_3 DOUBLE,
            STOCHRSIk_16_14_3_3 DOUBLE,
            STOCHRSId_16_14_3_3 DOUBLE,
            WPR DOUBLE,
            load_time TIMESTAMP
        )
    """)

    _logger.warning(f"Table {STOCKS_CURATED_TABLE_NAME} created in DuckDB")

def initialize():
    argparser = argparse.ArgumentParser(description="Create the stock database.")
    argparser.add_argument("-y", action="store_true", help="Initialisation without confirmation")

    if not argparser.parse_args().y:
        confirmation = input("Are you sure you want to create a new stock database? (yes/no): ")
        if confirmation.lower() != "yes":
            _logger.warning("Initialisation cancelled.")
            return
    else:
        _logger.info("Flagged to proceed with initialisation without confirmation.")
        
    load_dotenv("config/.env")
    _create_data_directory()
    with StockDuckDbConn().current_conn as conn:
        _create_tables(conn)

if __name__ == "__main__":
    initialize()