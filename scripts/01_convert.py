"""Step 01: Convert source files (Excel) in raw/ to Parquet in clean/."""
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from lib.convert import (
    DEFAULT_CHUNK_SIZE,
    _schema_defines_source_system,
    coerce_all_to_string,
    convert_excel_to_parquet,
)
from lib.fingerprint import (
    clear_step01_skipped_sentinel,
    get_changed_files,
    prune_orphan_clean_parquets,
    save_state,
    touch_step01_skipped_sentinel,
)
from lib.logging_util import setup_logging
from lib.paths import CLEAN_DIR, DATASET_ROOT, LOGS_DIR, RAW_DIR, SCHEMA_PATH
from lib.schema import load_schema

log = logging.getLogger(__name__)


def _add_source_system_from_schema() -> bool:
    try:
        if SCHEMA_PATH.exists():
            return _schema_defines_source_system(load_schema(SCHEMA_PATH))
    except Exception:
        pass
    return False


if __name__ == "__main__":
    setup_logging(LOGS_DIR)
    if not RAW_DIR.is_dir():
        print(f"No raw/ directory at {RAW_DIR}", file=sys.stderr)
        sys.exit(1)

    clear_step01_skipped_sentinel(DATASET_ROOT)

    changed, unchanged, state = get_changed_files(
        DATASET_ROOT, force=os.getenv("PIPELINE_FORCE") == "1"
    )

    pruned = prune_orphan_clean_parquets(DATASET_ROOT)
    for pq in pruned:
        log.info("Removed orphan clean Parquet (no matching raw): %s", pq.name)

    if not changed and not pruned:
        if not unchanged:
            log.info("No Excel files in raw/")
            save_state(DATASET_ROOT, state)
            touch_step01_skipped_sentinel(DATASET_ROOT)
            sys.exit(0)
        log.info(
            "No new or changed files. Skipping convert. (%d unchanged)", len(unchanged)
        )
        save_state(DATASET_ROOT, state)
        touch_step01_skipped_sentinel(DATASET_ROOT)
        sys.exit(0)

    if not changed and pruned:
        save_state(DATASET_ROOT, state)
        sys.exit(0)

    log.info(
        "Processing %d changed file(s), skipping %d unchanged",
        len(changed),
        len(unchanged),
    )

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    add_source_system = _add_source_system_from_schema()

    try:
        for filepath in changed:
            log.info("Converting %s", filepath.name)
            out_path = CLEAN_DIR / f"{filepath.stem}.parquet"
            stem = filepath.stem
            src_sys = stem if add_source_system else None
            if filepath.suffix.lower() == ".xlsx":
                rows, _headers, chunk_count = convert_excel_to_parquet(
                    filepath,
                    out_path,
                    chunk_size=DEFAULT_CHUNK_SIZE,
                    source_system=src_sys,
                )
            else:
                df = pd.read_excel(
                    filepath,
                    engine="xlrd"
                    if filepath.suffix.lower() == ".xls"
                    else "openpyxl",
                )
                df = coerce_all_to_string(df)
                if src_sys is not None:
                    df["source_system"] = src_sys
                    df["source_system"] = df["source_system"].astype("string")
                df.to_parquet(out_path, index=False)
                rows = len(df)
                chunk_count = 1
            log.info(
                "Converted %s: %s rows, %s chunk(s)",
                filepath.name,
                f"{rows:,}",
                chunk_count,
            )
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        # Include filepath in error message if available
        file_info = f" ({filepath.name})" if 'filepath' in dir() else ""
        print(f"Failed to read Excel{file_info}: {e}", file=sys.stderr)
        log.exception("Convert failed for %s", filepath.name if 'filepath' in dir() else "unknown file")
        sys.exit(1)

    save_state(DATASET_ROOT, state)
