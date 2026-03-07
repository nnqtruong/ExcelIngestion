"""
Build powerbi/warehouse.duckdb from Parquet files and create analytical views.
Deletes and recreates the database on every run.
"""
from pathlib import Path

import duckdb

# Paths relative to this script's parent directory (powerbi/); project root = parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent

DUCKDB_PATH = SCRIPT_DIR / "warehouse.duckdb"
TASKS_PARQUET = ROOT / "datasets" / "tasks" / "analytics" / "combined.parquet"
EMPLOYEES_PARQUET = ROOT / "datasets" / "dept_mapping" / "analytics" / "combined.parquet"


def main() -> None:
    # Delete and recreate DuckDB
    if DUCKDB_PATH.exists():
        DUCKDB_PATH.unlink()
    for wal in SCRIPT_DIR.glob("warehouse.duckdb.wal"):
        wal.unlink(missing_ok=True)

    conn = duckdb.connect(str(DUCKDB_PATH))

    # Load Parquet into tables
    conn.execute(
        "CREATE TABLE tasks AS SELECT * FROM read_parquet(?)",
        [str(TASKS_PARQUET)],
    )
    conn.execute(
        "CREATE TABLE employees AS SELECT * FROM read_parquet(?)",
        [str(EMPLOYEES_PARQUET)],
    )

    # v_task_duration — duration_minutes, duration_hours, lifecycle_hours
    conn.execute("""
        CREATE VIEW v_task_duration AS
        SELECT *,
            ROUND((epoch(endtime) - epoch(starttime)) / 60.0, 2) AS duration_minutes,
            ROUND((epoch(endtime) - epoch(starttime)) / 3600.0, 2) AS duration_hours,
            ROUND((epoch(dateended) - epoch(dateinitiated)) / 3600.0, 2) AS lifecycle_hours
        FROM tasks
        WHERE endtime IS NOT NULL AND starttime IS NOT NULL
    """)

    # v_daily_volume — by date(dateinitiated), tasks_initiated, tasks_completed, in_progress
    conn.execute("""
        CREATE VIEW v_daily_volume AS
        SELECT
            CAST(dateinitiated AS DATE) AS task_date,
            COUNT(*) AS tasks_initiated,
            SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS tasks_completed,
            SUM(CASE WHEN taskstatus = 'In Progress' THEN 1 ELSE 0 END) AS in_progress
        FROM tasks
        GROUP BY CAST(dateinitiated AS DATE)
    """)

    # v_drawer_summary — by drawer, total_tasks, completed, avg_duration_hours
    conn.execute("""
        CREATE VIEW v_drawer_summary AS
        SELECT
            drawer,
            COUNT(*) AS total_tasks,
            SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS completed,
            ROUND(AVG(CASE WHEN endtime IS NOT NULL AND starttime IS NOT NULL
                THEN (epoch(endtime) - epoch(starttime)) / 3600.0 END), 2) AS avg_duration_hours
        FROM tasks
        GROUP BY drawer
    """)

    # v_carrier_workload — by carrier, flowname, task_count, completed, in_progress, pending
    conn.execute("""
        CREATE VIEW v_carrier_workload AS
        SELECT
            carrier,
            flowname,
            COUNT(*) AS task_count,
            SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN taskstatus = 'In Progress' THEN 1 ELSE 0 END) AS in_progress,
            SUM(CASE WHEN taskstatus = 'Pending' THEN 1 ELSE 0 END) AS pending
        FROM tasks
        GROUP BY carrier, flowname
    """)

    # v_tasks_by_department — LEFT JOIN employees on LOWER(assignedto) = LOWER(userid)
    conn.execute("""
        CREATE VIEW v_tasks_by_department AS
        SELECT
            t.*,
            e.full_name,
            e.title AS employee_title,
            e.division,
            e.team
        FROM tasks t
        LEFT JOIN employees e ON LOWER(t.assignedto) = LOWER(e.userid)
    """)

    # v_team_workload — INNER JOIN, group by team, division, total_tasks, completed, avg_duration_hours
    conn.execute("""
        CREATE VIEW v_team_workload AS
        SELECT
            e.team,
            e.division,
            COUNT(*) AS total_tasks,
            SUM(CASE WHEN t.taskstatus = 'Completed' THEN 1 ELSE 0 END) AS completed,
            ROUND(AVG(CASE WHEN t.endtime IS NOT NULL AND t.starttime IS NOT NULL
                THEN (epoch(t.endtime) - epoch(t.starttime)) / 3600.0 END), 2) AS avg_duration_hours
        FROM tasks t
        INNER JOIN employees e ON LOWER(t.assignedto) = LOWER(e.userid)
        GROUP BY e.team, e.division
    """)

    # v_missing_status — tasks where taskstatus IS NULL
    conn.execute("""
        CREATE VIEW v_missing_status AS
        SELECT * FROM tasks WHERE taskstatus IS NULL
    """)

    conn.close()

    # Print table names, view names, and row counts
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    tables = conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """).fetchall()
    views = conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'VIEW'
        ORDER BY table_name
    """).fetchall()

    print("Tables:")
    for (name,) in tables:
        n = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"  {name}: {n} rows")

    print("\nViews:")
    for (name,) in views:
        n = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"  {name}: {n} rows")

    conn.close()
    print(f"\nDone. Database: {DUCKDB_PATH}")


if __name__ == "__main__":
    main()
