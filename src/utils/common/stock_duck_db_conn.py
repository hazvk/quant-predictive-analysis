import os
from pathlib import Path

import duckdb

class StockDuckDbConn():
    
    _data_path: str = None
    _access_mode: str = None
    _conn: duckdb.DuckDBPyConnection = None

    def __init__(self, access_mode: str = "READ_WRITE"):
        ingestion_path = os.environ["QUANT_PRED_ANALYSIS_DUCKDB_STORAGE_PATH"]
        self._data_path = Path(ingestion_path) / os.environ["STOCKS_DUCKDB_FILENAME"]
        self._access_mode = access_mode

    @property
    def data_path(self) -> str:
        return str(self._data_path)

    @property
    def current_conn(self) -> duckdb.DuckDBPyConnection:
        self._conn or self.reset_conn() # reset if not defined
        return self._conn
    
    def reset_conn(self):
        if self._conn:
            self._conn.close()
        self._conn = duckdb.connect(self._data_path, config={'access_mode': self._access_mode})
        return