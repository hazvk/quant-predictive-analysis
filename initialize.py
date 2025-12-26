import argparse
import logging
import os
from pathlib import Path
from dotenv import load_dotenv
import duckdb

from src.utils.constants import STOCKS_RAW_TABLE_NAME
from src.utils.stock_duck_db_conn import StockDuckDbConn

def _create_data_directory():
    ingestion_path = os.environ["QUANT_PRED_ANALYSIS_DUCKDB_DATA_PATH"]
    try:
        Path(ingestion_path).mkdir(parents=True)
        logging.warning(f"Created data directory {ingestion_path}")
    except FileExistsError:
        logging.warning(f"Data directory {ingestion_path} already exists")

def _create_table_if_not_exists(duckdb_conn: duckdb.DuckDBPyConnection):
    duckdb_conn.execute(f"""
        CREATE TABLE {STOCKS_RAW_TABLE_NAME} (
            Datetime TIMESTAMP,
            Open DOUBLE,
            High DOUBLE,
            Low DOUBLE,
            Close DOUBLE,
            Volume BIGINT,
            Ingestion_Time TIMESTAMP
        )
    """)

    logging.warning(f"Table {STOCKS_RAW_TABLE_NAME} created in DuckDB")

def initialize():
    argparser = argparse.ArgumentParser(description="Create the stock database.")
    argparser.add_argument("-y", action="store_true", help="Initialisation without confirmation")

    if not argparser.parse_args().y:
        confirmation = input("Are you sure you want to create a new stock database? (yes/no): ")
        if confirmation.lower() != "yes":
            print("Initialisation cancelled.")
            return
    else:
        print("Flagged to proceed with initialisation without confirmation.")
        
    load_dotenv()
    _create_data_directory()
    with StockDuckDbConn().get_current_conn() as conn:
        _create_table_if_not_exists(conn)

if __name__ == "__main__":
    initialize()