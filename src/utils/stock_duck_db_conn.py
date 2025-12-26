import os
from pathlib import Path

import duckdb


class StockDuckDbConn():
    
    data_path: str = None
    _conn: duckdb.DuckDBPyConnection = None

    def __init__(self):
        ingestion_path = os.environ["QUANT_PRED_ANALYSIS_DUCKDB_DATA_PATH"]
        Path(ingestion_path).mkdir(parents=True, exist_ok=True)
        self.data_path = Path(ingestion_path) / "stocks.duckdb"

    def get_current_conn(self) -> duckdb.DuckDBPyConnection:
        self._conn = self._conn or duckdb.connect(self.data_path)
        return self._conn