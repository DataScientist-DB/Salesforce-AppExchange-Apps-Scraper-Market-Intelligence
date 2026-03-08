# logger.py
import logging
import os
import sys

LOG_LEVEL = os.getenv("SFE_LOG_LEVEL", "INFO").upper()


def _create_root_logger() -> logging.Logger:
    logger = logging.getLogger("sfe")
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(LOG_LEVEL)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


logger = _create_root_logger()


def get_logger(name: str) -> logging.Logger:
    """
    Get a child logger: sfe.<name>
    Usage: log = get_logger(__name__)
    """
    return logger.getChild(name)
