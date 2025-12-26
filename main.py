from dotenv import load_dotenv

from src.ingestion.specified_tickers.nvda_ingestor import NvdaIngestor



def main():
    load_dotenv()
    NvdaIngestor().ingest_stock_data("2025-11-20", "2025-12-21")


if __name__ == "__main__":
    main()
