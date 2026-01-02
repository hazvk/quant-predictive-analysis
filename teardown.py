import os
from dotenv import load_dotenv
import argparse

from src.utils.logger import Logger
from src.utils.common.stock_duck_db_conn import StockDuckDbConn

_logger = Logger()

def teardown():
    argparser = argparse.ArgumentParser(description="Teardown the stock database.")
    argparser.add_argument("-y", action="store_true", help="Delete without confirmation")

    if not argparser.parse_args().y:
        confirmation = input("Are you sure you want to delete the stock database? (yes/no): ")
        if confirmation.lower() != "yes":
            _logger.warning("Teardown cancelled.")
            return
    else:
        _logger.info("Flagged to proceed with teardown without confirmation.")
        
    load_dotenv("config/.env")
    if os.path.exists(StockDuckDbConn().data_path):
        os.remove(StockDuckDbConn().data_path)
        _logger.warning("Removed stock data directory")
    else:
        _logger.warning("Stock data directory does not exist")

if __name__ == "__main__":
    teardown()