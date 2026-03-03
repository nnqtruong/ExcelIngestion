"""Step 05: Apply value_maps.yaml to standardize categorical values."""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.logging_util import setup_logging
from lib.normalize_values import run_normalize_values
from lib.paths import CLEAN_DIR, LOGS_DIR, VALUE_MAPS_PATH

if __name__ == "__main__":
    setup_logging(LOGS_DIR)
    log = logging.getLogger(__name__)
    try:
        n = run_normalize_values(CLEAN_DIR, VALUE_MAPS_PATH, log)
    except ValueError as e:
        log.error("%s", e)
        sys.exit(1)
    if not CLEAN_DIR.is_dir():
        log.error("No clean/ directory at %s", CLEAN_DIR)
        sys.exit(1)
    if n == 0:
        log.info("No value maps in %s or no Parquet files in clean/", VALUE_MAPS_PATH)
    else:
        log.info("Finished normalizing values for %d file(s).", n)
