from dotenv import load_dotenv
from src.utils.stock_duck_db_conn import StockDuckDbConn


load_dotenv()

duck_db_conn = StockDuckDbConn().get_current_conn()

# example usage
duck_db_conn.sql("SELECT * from stocks_raw;")