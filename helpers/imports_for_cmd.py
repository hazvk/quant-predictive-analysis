# load everything you need using
# from helpers.imports_for_cmd import *

from dotenv import load_dotenv

from src.utils.stock_duck_db_conn import StockDuckDbConn
from helpers.general_stock_ingestor import GeneralStockIngestor


load_dotenv()

duck_db_conn = StockDuckDbConn().get_current_conn()

# example usage
# duck_db_conn.sql(f"SELECT * from {STOCKS_RAW_TABLE_NAME}")