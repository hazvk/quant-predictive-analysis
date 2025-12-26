from dotenv import load_dotenv

from src.ingestion.specified_tickers.nvda_ingestor import NvdaIngestor


load_dotenv()

def _ingest_data():
    NvdaIngestor().ingest_stock_data("2025-11-20", "2025-12-21")

def main():
    _ingest_data()

if __name__ == "__main__":
    main()
