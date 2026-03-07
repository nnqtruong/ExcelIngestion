"""Step 08: Final gate. Validate row count, required columns, duplicates, null rates, dtypes; output report."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.config import get_combined_path
from lib.logging_util import setup_logging
from lib.paths import ANALYTICS_DIR, COMBINE_PATH, LOGS_DIR, REPORT_PATH, SCHEMA_PATH
from lib.validate import run_validate

if __name__ == "__main__":
    setup_logging(LOGS_DIR)
    combined_path = get_combined_path(ANALYTICS_DIR, COMBINE_PATH)
    try:
        report = run_validate(combined_path, SCHEMA_PATH, COMBINE_PATH, REPORT_PATH)
    except FileNotFoundError as e:
        print(f"Validation failed: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"Validation report written to {REPORT_PATH}")
    if report["passed"]:
        print("All checks passed.")
    else:
        print("Validation failed: one or more checks breached thresholds.", file=sys.stderr)
        for check_name, check in report["checks"].items():
            if not check.get("passed", True):
                print(f"  FAIL: {check_name} - {check}", file=sys.stderr)
        sys.exit(1)
