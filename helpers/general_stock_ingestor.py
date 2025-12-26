# only use for exploration, not in scalable pipeline
# Example use: GeneralStockIngestor("NVDA")._download_stock_data("2025-11-20", "2025-12-21", "15m")

from src.ingestion.stock_ingestor import StockIngestor


class GeneralStockIngestor(StockIngestor):
    
    _ticker: str = None

    def __init__(self, ticker: str):
        super().__init__()
        self._ticker = ticker

    @property
    def ticker(self) -> str:
        return self._ticker

    def _insert_stock_data(self, duckdb_conn, downloaded_stock_data_df):
        # Override to skip actual insertion during exploration
        raise NotImplementedError("This method is not implemented for GeneralStockIngestor, since it is only for exploration purposes.")