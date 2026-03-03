"""Step 10: Create SQL views in SQLite database for common analytics queries."""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.config import get_sqlite_db_path
from lib.logging_util import setup_logging
from lib.paths import DATASET_ROOT, LOGS_DIR
from lib.sqlite_views import run_sqlite_views

if __name__ == "__main__":
    setup_logging(LOGS_DIR)
    log = logging.getLogger(__name__)
    db_path = get_sqlite_db_path(DATASET_ROOT)
    try:
        results = run_sqlite_views(db_path, log)
    except FileNotFoundError as e:
        log.error("%s (run step 09 first)", e)
        sys.exit(1)
    print(f"\nSQLite Views Summary:")
    print(f"  Database: {db_path}")
    print(f"  Views created: {len(results)}")
    print(f"\n  View row counts:")
    for view_name, count in results.items():
        print(f"    {view_name}: {count} rows")
    log.info("Finished creating SQLite views in %s", db_path.name)
