"""Pipeline logging: file + stdout; step performance decorator."""
import functools
import logging
import sys
import time
from pathlib import Path

import psutil

logger = logging.getLogger(__name__)

# 500K row guideline: flag if any step exceeds this (MB)
RAM_MB_LIMIT = 1024


def monitor_step(func):
    """Decorator: log elapsed time and RAM before/after (and delta). Flag if peak > RAM_MB_LIMIT."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process()
        mem_before = process.memory_info().rss / 1024 / 1024
        start = time.time()

        result = func(*args, **kwargs)

        elapsed = time.time() - start
        mem_after = process.memory_info().rss / 1024 / 1024
        delta = mem_after - mem_before

        logger.info(
            "%s | %.1fs | RAM: %.0fMB -> %.0fMB (delta: %.0fMB)",
            func.__name__,
            elapsed,
            mem_before,
            mem_after,
            delta,
        )
        if mem_after > RAM_MB_LIMIT:
            logger.warning(
                "%s exceeded %.0fMB RAM limit (%.0fMB); consider optimizing for 500K+ row files.",
                func.__name__,
                RAM_MB_LIMIT,
                mem_after,
            )
        return result
    return wrapper


def setup_logging(logs_dir: Path) -> None:
    """Configure logging to pipeline.log and console."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "pipeline.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
