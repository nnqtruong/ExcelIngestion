"""Step 09: Export combined Parquet to SQLite database with indexes."""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.config import get_combined_path, get_sqlite_db_path, get_sqlite_table_name
from lib.export_sqlite import run_export_sqlite
from lib.logging_util import setup_logging
from lib.paths import ANALYTICS_DIR, COMBINE_PATH, DATASET_ROOT, LOGS_DIR

if __name__ == "__main__":
    setup_logging(LOGS_DIR)
    log = logging.getLogger(__name__)
    combined_path = get_combined_path(ANALYTICS_DIR, COMBINE_PATH)
    db_path = get_sqlite_db_path(DATASET_ROOT)
    table_name = get_sqlite_table_name(DATASET_ROOT)
    try:
        results = run_export_sqlite(combined_path, db_path, table_name, log)
    except FileNotFoundError as e:
        log.error("%s (run step 06 first)", e)
        sys.exit(1)
    print(f"\nSQLite Export Summary:")
    print(f"  Database: {db_path}")
    print(f"  Total rows: {results['total_rows']}")
    if "unique_tasks" in results:
        print(f"  Unique taskids: {results['unique_tasks']}")
    if "status_breakdown" in results:
        print(f"\n  Status breakdown:")
        for status, count in results["status_breakdown"]:
            print(f"    {status if status else '(NULL)'}: {count}")
    log.info("Finished SQLite export to %s", db_path.name)
