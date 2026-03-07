"""Step 10: Create SQL views in SQLite for analytics."""
import logging
import sqlite3
from pathlib import Path

from lib.logging_util import monitor_step

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
        SELECT carrier, flowname,
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
    "v_tasks_by_department": """
        CREATE VIEW IF NOT EXISTS v_tasks_by_department AS
        SELECT
            t.*,
            e.full_name,
            e.title AS employee_title,
            e.division,
            e.team,
            e.divisionid
        FROM tasks t
        LEFT JOIN employees e
            ON LOWER(t.assignedto) = LOWER(e.userid)
            OR LOWER(t.operationby) = LOWER(e.userid)
            OR LOWER(t.taskfrom) = LOWER(e.userid)
    """,
}


def create_views(conn: sqlite3.Connection, log: logging.Logger) -> list[str]:
    """Create analytics views; skip any that fail (e.g. missing columns). Returns list of created view names."""
    cursor = conn.cursor()
    created = []
    for view_name, sql in VIEWS.items():
        try:
            cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
            cursor.execute(sql)
            created.append(view_name)
            log.info("Created view: %s", view_name)
        except sqlite3.Error as e:
            log.warning("Skipped view %s (dataset may not have required columns): %s", view_name, e)
    conn.commit()
    return created


def verify_views(conn: sqlite3.Connection, view_names: list[str], log: logging.Logger) -> dict:
    """Verify views are queryable; return {view_name: row_count}. Only checks view_names that exist."""
    cursor = conn.cursor()
    results = {}
    for view_name in view_names:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
            results[view_name] = cursor.fetchone()[0]
            log.info("View %s: %d rows", view_name, results[view_name])
        except sqlite3.Error as e:
            log.warning("Failed to query view %s: %s", view_name, e)
            results[view_name] = -1
    return results


@monitor_step
def run_sqlite_views(db_path: Path, log: logging.Logger) -> dict:
    """Create and verify views in db_path. Returns verify results for created views only."""
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")
    conn = sqlite3.connect(db_path)
    created = create_views(conn, log)
    results = verify_views(conn, created, log) if created else {}
    conn.close()
    return results
