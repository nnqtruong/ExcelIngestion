"""Step 01: Convert source files (Excel) in raw/ to Parquet in clean/."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.convert import run_convert
from lib.logging_util import setup_logging
from lib.paths import CLEAN_DIR, LOGS_DIR, RAW_DIR

if __name__ == "__main__":
    setup_logging(LOGS_DIR)
    try:
        n = run_convert(RAW_DIR, CLEAN_DIR, verbose=True)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Failed to read Excel: {e}", file=sys.stderr)
        sys.exit(1)
    if not n:
        print("No Excel files in raw/")
