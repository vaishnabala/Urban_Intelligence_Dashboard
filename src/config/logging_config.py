"""
Centralized logging configuration using loguru.
Logs to both console and file (logs/app.log).
"""

import sys
from pathlib import Path
from loguru import logger

# Remove default logger
logger.remove()

# Log directory
LOG_DIR = Path(__file__).resolve().parent.parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "app.log"

# Console logging — INFO and above, colored
logger.add(
    sys.stdout,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
    colorize=True,
)

# File logging — DEBUG and above, rotated daily, kept 7 days
logger.add(
    str(LOG_FILE),
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{function}:{line} - {message}",
    rotation="10 MB",
    retention="7 days",
    compression="zip",
    encoding="utf-8",
)

logger.info("Logging initialized — console + file: {}", LOG_FILE)