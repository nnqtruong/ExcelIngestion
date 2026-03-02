"""Step 10: Create SQL views in SQLite database for common analytics queries."""

import logging
import sqlite3
import sys
from pathlib import Path

# Project root (parent of scripts/)
ROOT = Path(__file__).resolve().parent.parent
ANALYTICS_DIR = ROOT / "analytics"
LOGS_DIR = ROOT / "logs"
DB_PATH = ANALYTICS_DIR / "tasks.db"

VIEWS = {
    "v_task_duration": """
        CREATE VIEW IF NOT EXISTS v_task_duration AS
        SELECT *,
            ROUND((julianday(endtime) - julianday(starttime)) * 24, 2) AS duration_hours,
            ROUND((julianday(dateended) - julianday(dateinitiated)) * 24, 2) AS total_lifecycle_hours
        FROM tasks
        WHERE endtime IS NOT NULL AND starttime IS NOT NULL
    """,
    "v_daily_volume": """
        CREATE VIEW IF NOT EXISTS v_daily_volume AS
        SELECT DATE(dateinitiated) AS task_date,
            COUNT(*) AS tasks_initiated,
            SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS tasks_completed
        FROM tasks
        GROUP BY DATE(dateinitiated)
    """,
    "v_drawer_summary": """
        CREATE VIEW IF NOT EXISTS v_drawer_summary AS
        SELECT drawer,
            COUNT(*) AS total_tasks,
            SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS completed,
            ROUND(AVG(CASE WHEN endtime IS NOT NULL AND starttime IS NOT NULL
                THEN (julianday(endtime) - julianday(starttime)) * 24 END), 2) AS avg_duration_hours
        FROM tasks
        GROUP BY drawer
    """,
    "v_carrier_workload": """
        CREATE VIEW IF NOT EXISTS v_carrier_workload AS
        SELECT carrier,
            flowname,
            COUNT(*) AS task_count,
            SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN taskstatus = 'In Progress' THEN 1 ELSE 0 END) AS in_progress,
            SUM(CASE WHEN taskstatus = 'Pending' THEN 1 ELSE 0 END) AS pending
        FROM tasks
        GROUP BY carrier, flowname
    """,
    "v_missing_status": """
        CREATE VIEW IF NOT EXISTS v_missing_status AS
        SELECT * FROM tasks WHERE taskstatus IS NULL
    """,
}


def setup_logging() -> None:
    """Configure logging to pipeline.log and console."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / "pipeline.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def create_views(conn: sqlite3.Connection, log: logging.Logger) -> list[str]:
    """Create all analytics views. Returns list of created view names."""
    cursor = conn.cursor()
    created = []

    for view_name, sql in VIEWS.items():
        try:
            # Drop existing view first to ensure fresh creation
            cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
            cursor.execute(sql)
            created.append(view_name)
            log.info("Created view: %s", view_name)
        except sqlite3.Error as e:
            log.error("Failed to create view %s: %s", view_name, e)
            raise

    conn.commit()
    return created


def verify_views(conn: sqlite3.Connection, log: logging.Logger) -> dict:
    """Verify views are queryable and return sample counts."""
    cursor = conn.cursor()
    results = {}

    for view_name in VIEWS:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
            count = cursor.fetchone()[0]
            results[view_name] = count
            log.info("View %s: %d rows", view_name, count)
        except sqlite3.Error as e:
            log.error("Failed to query view %s: %s", view_name, e)
            results[view_name] = -1

    return results


def main() -> None:
    setup_logging()
    log = logging.getLogger(__name__)

    if not DB_PATH.exists():
        log.error("SQLite database not found: %s (run step 09 first)", DB_PATH)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    # Create views
    created = create_views(conn, log)
    log.info("Created %d views", len(created))

    # Verify views
    results = verify_views(conn, log)

    conn.close()

    # Print summary
    print(f"\nSQLite Views Summary:")
    print(f"  Database: {DB_PATH}")
    print(f"  Views created: {len(created)}")
    print(f"\n  View row counts:")
    for view_name, count in results.items():
        print(f"    {view_name}: {count} rows")

    log.info("Finished creating SQLite views in %s", DB_PATH.name)


if __name__ == "__main__":
    main()
