import logging
import os
from dotenv import load_dotenv
import argparse

from src.utils.stock_duck_db_conn import StockDuckDbConn

def teardown():
    argparser = argparse.ArgumentParser(description="Teardown the stock database.")
    argparser.add_argument("-y", action="store_true", help="Delete without confirmation")

    if not argparser.parse_args().y:
        confirmation = input("Are you sure you want to delete the stock database? (yes/no): ")
        if confirmation.lower() != "yes":
            print("Teardown cancelled.")
            return
    else:
        print("Flagged to proceed with teardown without confirmation.")
        
    load_dotenv("config/.env")
    if os.path.exists(StockDuckDbConn().data_path):
        os.remove(StockDuckDbConn().data_path)
        logging.warning("Removed stock data directory")
    else:
        logging.warning("Stock data directory does not exist")

if __name__ == "__main__":
    teardown()