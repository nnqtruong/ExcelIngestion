"""Step 06: Union cleaned Parquet files into one dataset with row_id."""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.combine_datasets import run_combine_datasets
from lib.logging_util import setup_logging
from lib.paths import ANALYTICS_DIR, CLEAN_DIR, COMBINE_PATH, LOGS_DIR

if __name__ == "__main__":
    setup_logging(LOGS_DIR)
    log = logging.getLogger(__name__)
    try:
        output_path = run_combine_datasets(CLEAN_DIR, ANALYTICS_DIR, COMBINE_PATH, log)
    except (FileNotFoundError, ValueError) as e:
        log.error("%s", e)
        sys.exit(1)
    log.info("Finished combine: output written to %s", output_path)
