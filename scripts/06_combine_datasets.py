"""Step 06: Union cleaned Parquet files into one dataset with row_id."""
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.combine_datasets import run_combine_datasets
from lib.config import get_combined_path
from lib.fingerprint import STEP01_SKIPPED_SENTINEL
from lib.logging_util import setup_logging
from lib.paths import ANALYTICS_DIR, CLEAN_DIR, COMBINE_PATH, DATASET_ROOT, LOGS_DIR

if __name__ == "__main__":
    setup_logging(LOGS_DIR)
    log = logging.getLogger(__name__)

    combined_path = get_combined_path(ANALYTICS_DIR, COMBINE_PATH)
    sentinel = DATASET_ROOT / STEP01_SKIPPED_SENTINEL
    pipeline_from_step = int(os.getenv("PIPELINE_FROM_STEP", "1"))
    step01_ran_this_run = pipeline_from_step <= 1
    env_step01_skipped = os.getenv("PIPELINE_STEP01_SKIPPED") == "1"
    force = os.getenv("PIPELINE_FORCE") == "1"

    if (
        not force
        and step01_ran_this_run
        and combined_path.exists()
        and (env_step01_skipped or sentinel.exists())
    ):
        log.info(
            "Step 01 skipped (no raw changes); %s exists. Skipping combine.",
            combined_path.name,
        )
        sys.exit(0)

    try:
        output_path = run_combine_datasets(CLEAN_DIR, ANALYTICS_DIR, COMBINE_PATH, log)
    except (FileNotFoundError, ValueError) as e:
        log.error("%s", e)
        sys.exit(1)
    log.info("Finished combine: output written to %s", output_path)
