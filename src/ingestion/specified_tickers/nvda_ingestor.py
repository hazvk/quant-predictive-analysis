from ..stock_ingestor import StockIngestor

class NvdaIngestor(StockIngestor):
    
    @property
    def ticker(self):
        return "NVDA"