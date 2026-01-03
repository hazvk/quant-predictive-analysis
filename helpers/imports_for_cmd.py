# load everything you need using
# from helpers.imports_for_cmd import *

from dotenv import load_dotenv

from src.utils.common import *
from src.utils.common.stock_duck_db_conn import StockDuckDbConn


load_dotenv("config/.env")

stock_duckdb_reader_conn = StockDuckDbConn(access_mode="READ_ONLY")

# example usage
# stock_duckdb_reader_conn.current_conn.sql(f"SELECT * from {STOCKS_RAW_TABLE_NAME}")