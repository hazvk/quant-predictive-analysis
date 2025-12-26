import logging
import os
from datetime import datetime
from pathlib import Path

class Logger:
    def __init__(self):
        self.name = "quant-predictive-analysis"
        self.log_dir = Path(os.environ["QUANT_PRED_ANALYSIS_DUCKDB_STORAGE_PATH"] / "logs")
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        # Create logs directory if it doesn't exist
        os.makedirs(self.log_dir, exist_ok=True)

        logger = logging.getLogger(self.name)
        logger.setLevel(logging.DEBUG)

        # File handler
        log_file = os.path.join(
            self.log_dir, f"{self.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def debug(self, msg: str) -> None:
        self.logger.debug(msg)

    def info(self, msg: str) -> None:
        self.logger.info(msg)

    def warning(self, msg: str) -> None:
        self.logger.warning(msg)

    def error(self, msg: str) -> None:
        self.logger.error(msg)

    def critical(self, msg: str) -> None:
        self.logger.critical(msg)