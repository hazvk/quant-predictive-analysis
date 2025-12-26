from dotenv import load_dotenv

from src.utils.constants import STOCKS_RAW_TABLE_NAME
from src.utils.stock_duck_db_conn import StockDuckDbConn


load_dotenv()

duck_db_conn = StockDuckDbConn().get_current_conn()

# example usage
duck_db_conn.sql(f"SELECT * from {STOCKS_RAW_TABLE_NAME}")