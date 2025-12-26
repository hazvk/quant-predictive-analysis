from dotenv import load_dotenv
import duckdb

from src.utils.constants import STOCKS_RAW_TABLE_NAME
from src.utils.stock_duck_db_conn import StockDuckDbConn

def _create_table_if_not_exists(duckdb_conn: duckdb.DuckDBPyConnection):
    duckdb_conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {STOCKS_RAW_TABLE_NAME} (
            Datetime TIMESTAMP,
            Open DOUBLE,
            High DOUBLE,
            Low DOUBLE,
            Close DOUBLE,
            Volume BIGINT,
            Ingestion_Time TIMESTAMP
        )
    """)

def initialize():
    load_dotenv()
    with StockDuckDbConn().get_current_conn() as conn:
        _create_table_if_not_exists(conn)

if __name__ == "__main__":
    initialize()