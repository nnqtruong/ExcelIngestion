"""Step 04: Cast columns to schema types; flag failing rows to errors/ sidecar."""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.clean_errors import run_clean_errors
from lib.logging_util import setup_logging
from lib.paths import CLEAN_DIR, ERRORS_DIR, LOGS_DIR, SCHEMA_PATH

if __name__ == "__main__":
    setup_logging(LOGS_DIR)
    log = logging.getLogger(__name__)
    try:
        n = run_clean_errors(CLEAN_DIR, ERRORS_DIR, SCHEMA_PATH, log)
    except (FileNotFoundError, ValueError) as e:
        log.error("%s", e)
        sys.exit(1)
    if not CLEAN_DIR.is_dir():
        log.error("No clean/ directory at %s", CLEAN_DIR)
        sys.exit(1)
    if n == 0:
        log.info("No Parquet files in clean/")
    else:
        log.info("Finished casting and flagging errors for %d file(s).", n)
