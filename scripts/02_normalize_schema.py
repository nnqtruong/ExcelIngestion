"""Step 02: Normalize column names and types to target schema."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.logging_util import setup_logging
from lib.normalize_schema import run_normalize_schema
from lib.paths import CLEAN_DIR, LOGS_DIR, SCHEMA_PATH

if __name__ == "__main__":
    setup_logging(LOGS_DIR)
    try:
        n = run_normalize_schema(CLEAN_DIR, SCHEMA_PATH)
    except (FileNotFoundError, ValueError) as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    if not CLEAN_DIR.is_dir():
        print(f"No clean/ directory at {CLEAN_DIR}", file=sys.stderr)
        sys.exit(1)
    parquet_files = list(CLEAN_DIR.glob("*.parquet"))
    if not parquet_files:
        print("No Parquet files in clean/")
    else:
        for p in sorted(parquet_files):
            print(f"Normalizing {p.name} ...")
        print(f"Done. Normalized {n} file(s).")
