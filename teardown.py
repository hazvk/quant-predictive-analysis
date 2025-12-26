import logging
import os
from dotenv import load_dotenv

from src.utils.stock_duck_db_conn import StockDuckDbConn

def teardown():
    load_dotenv()
    if os.path.exists(StockDuckDbConn().data_path):
        os.remove(StockDuckDbConn().data_path)
        logging.warning("Removed stock data directory")
    else:
        logging.warning("Stock data directory does not exist")

if __name__ == "__main__":
    teardown()