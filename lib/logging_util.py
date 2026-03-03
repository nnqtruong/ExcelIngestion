"""Pipeline logging: file + stdout."""
import logging
import sys
from pathlib import Path


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
