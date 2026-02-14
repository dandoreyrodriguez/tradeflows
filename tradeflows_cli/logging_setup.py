##############################
# Util functions for logging #
##############################
# By: DDR

from __future__ import annotations
import logging
from pathlib import Path
from datetime import datetime, timezone

# logger has a common project name
LOGGER_NAME = "tradeflows"

def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)

def setup_logging(log_dir: Path, *, timestamped: bool = False) -> Path:
    """
    Set up logging configuation.
    - Console: INFO+
    - File: DEBUG+
    Returns log path.
    """
    log_dir.mkdir(parents=True, exist_ok=True)

    if timestamped:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%SZ")
        log_file = log_dir / f"tradeflows_{ts}.log"
    else:
        log_file = log_dir / "tradeflows.log"

    logger = get_logger()
    logger.setLevel(logging.DEBUG)

    # avoid duplicates 
    logger.handlers.clear()
    logger.propagate = False

    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    formatter = logging.Formatter(fmt)

    # set console 
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # set file
    file_handler = logging.FileHandler(log_file, encoding="utf-8") 
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return log_file




    
