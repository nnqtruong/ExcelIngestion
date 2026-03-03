"""Step 07: Apply null-filling strategies from schema (runs on analytics/ combined output)."""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.config import get_combined_path
from lib.handle_nulls import run_handle_nulls
from lib.logging_util import setup_logging
from lib.paths import ANALYTICS_DIR, COMBINE_PATH, LOGS_DIR, SCHEMA_PATH

if __name__ == "__main__":
    setup_logging(LOGS_DIR)
    log = logging.getLogger(__name__)
    combined_path = get_combined_path(ANALYTICS_DIR, COMBINE_PATH)
    try:
        run_handle_nulls(combined_path, SCHEMA_PATH, log)
    except FileNotFoundError as e:
        log.error("%s (run step 06 first)", e)
        sys.exit(1)
    log.info("Finished null handling for %s", combined_path.name)
