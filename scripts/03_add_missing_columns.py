"""Step 03: Add missing columns with null values and correct dtype per schema."""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.add_missing_columns import run_add_missing_columns
from lib.logging_util import setup_logging
from lib.paths import CLEAN_DIR, LOGS_DIR, SCHEMA_PATH

if __name__ == "__main__":
    setup_logging(LOGS_DIR)
    log = logging.getLogger(__name__)
    try:
        n = run_add_missing_columns(CLEAN_DIR, SCHEMA_PATH, log)
    except (FileNotFoundError, ValueError) as e:
        log.error("%s", e)
        sys.exit(1)
    if not CLEAN_DIR.is_dir():
        log.error("No clean/ directory at %s", CLEAN_DIR)
        sys.exit(1)
    if n == 0:
        log.info("No Parquet files in clean/")
    else:
        log.info("Finished adding missing columns for %d file(s).", n)
