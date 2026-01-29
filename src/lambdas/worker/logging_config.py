import logging
import os

def setup_logging() -> logging.Logger:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger(__name__)

import logging, os, sys

def setup_logging() -> logging.Logger:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, log_level, logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    # Lambda has a handler, so set level in handlers
    if root.handlers:
        for h in root.handlers:
            h.setLevel(level)
    else:
        h = logging.StreamHandler(sys.stdout)
        h.setLevel(level)
        fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        h.setFormatter(fmt)
        root.addHandler(h)

    return logging.getLogger(__name__)