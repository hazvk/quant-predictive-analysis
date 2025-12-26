import json
from dotenv import load_dotenv

from src.ingestion.stock_ingestor import StockIngestor
from src.curated.stock_curator import StockCurator


load_dotenv()

def _ingest_data():
    stocks_to_ingest = json.load(open("stocks_to_ingest.json"))
    for ticker in stocks_to_ingest:
        StockIngestor(ticker).ingest_stock_data("2025-09-25", "2025-12-26")

def _curate_data():
    stocks_to_ingest = json.load(open("stocks_to_ingest.json"))
    for ticker in stocks_to_ingest:
        StockCurator(ticker).curate_stock_data()

def main():
    _ingest_data()
    _curate_data()

if __name__ == "__main__":
    main()
