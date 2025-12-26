import os
from dotenv import load_dotenv

from src.utils.stock_duck_db_conn import StockDuckDbConn



def teardown():
    load_dotenv()
    os.remove(StockDuckDbConn().data_path)

if __name__ == "__main__":
    teardown()