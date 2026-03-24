"""Sync dbt mart definitions to SQLite views on the shared warehouse.

Delegates to lib.sqlite_views.run_sqlite_views (same logic as pipeline step 10).

Run after step 09 (export_sqlite) so base tables exist. Uses PIPELINE_ENV (default dev)
to pick analytics/dev_warehouse.db vs analytics/warehouse.db.

Usage:
  python scripts/sync_views_to_sqlite.py

Same behavior as pipeline step 10 (10_sqlite_views).
"""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.logging_util import setup_logging
from lib.sqlite_views import run_sqlite_views
from lib.sync_mart_views_sqlite import get_shared_warehouse_path

LOGS_DIR = ROOT / "datasets" / "dev" / "tasks" / "logs"


def main() -> int:
    setup_logging(LOGS_DIR)
    log = logging.getLogger(__name__)
    db_path = get_shared_warehouse_path(ROOT)
    log.info("Warehouse: %s", db_path)
    try:
        results = run_sqlite_views(db_path, log)
    except FileNotFoundError as e:
        log.error("%s", e)
        return 1
    print("\nSync mart views to SQLite")
    print(f"  Database: {db_path}")
    print(f"  Views verified: {len(results)}")
    for name, count in sorted(results.items()):
        print(f"    {name}: {count} rows")
    log.info("Finished sync_views_to_sqlite")
    return 0


if __name__ == "__main__":
    sys.exit(main())
