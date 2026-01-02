from dotenv import load_dotenv

from src.ingestion.stock_ingestor import StockIngestor
from src.curated.stock_curator import StockCurator
from src.utils.config import STOCKS_TO_CALCULATE


load_dotenv("config/.env")

def _ingest_data():
    for _, tickers in STOCKS_TO_CALCULATE.items():
        for ticker in tickers:
            StockIngestor(ticker).ingest_stock_data()

def _curate_data():
    for _, tickers in STOCKS_TO_CALCULATE.items():
        for ticker in tickers:
            StockCurator(ticker).curate_stock_data()

def main():
    _ingest_data()
    _curate_data()

if __name__ == "__main__":
    main()
