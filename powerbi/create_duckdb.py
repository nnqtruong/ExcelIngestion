"""Create DuckDB database from Parquet files for Power BI ODBC consumption.
Reads PIPELINE_ENV (default: dev). Dev uses dev_warehouse.duckdb; prod uses warehouse.duckdb.
Parquet paths: {DATA_ROOT}/{env}/tasks|dept_mapping/analytics/combined.parquet (same as the pipeline)."""
import os
import sys
from pathlib import Path

import duckdb

# Resolve paths relative to this script's parent (project root)
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.data_root import get_data_root, get_powerbi_path


def get_env() -> str:
    """Return PIPELINE_ENV (default: dev)."""
    return os.environ.get("PIPELINE_ENV") or "dev"


def _paths_for_env(env: str) -> tuple[Path, Path, Path]:
    """Return (DUCKDB_PATH, TASKS_PARQUET, DEPT_PARQUET) for the given environment."""
    data_root = get_data_root()
    db_name = "dev_warehouse.duckdb" if env == "dev" else "warehouse.duckdb"
    duckdb_path = get_powerbi_path() / db_name
    tasks_parquet = data_root / env / "tasks" / "analytics" / "combined.parquet"
    dept_parquet = data_root / env / "dept_mapping" / "analytics" / "combined.parquet"
    return duckdb_path, tasks_parquet, dept_parquet


def _escape_sql(s: str) -> str:
    return s.replace("'", "''")


def create_database() -> None:
    """Delete and recreate DuckDB database with tables and views."""
    env = get_env()
    duckdb_path, tasks_parquet, dept_parquet = _paths_for_env(env)
    print(f"Environment: {env}")
    print(f"Database: {duckdb_path}")
    print(f"Tasks parquet: {tasks_parquet}")
    print(f"Dept parquet: {dept_parquet}\n")

    # Delete old database
    if duckdb_path.exists():
        duckdb_path.unlink()
        print(f"Deleted existing: {duckdb_path}")

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(duckdb_path))

    # Load tasks table
    if tasks_parquet.exists():
        tasks_sql = _escape_sql(tasks_parquet.as_posix())
        conn.execute(f"CREATE TABLE tasks AS SELECT * FROM read_parquet('{tasks_sql}')")
        count = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        print(f"Loaded tasks: {count:,} rows")
    else:
        print(f"WARNING: Tasks parquet not found: {tasks_parquet}")

    # Load employees table (if exists)
    if dept_parquet.exists():
        dept_sql = _escape_sql(dept_parquet.as_posix())
        conn.execute(f"CREATE TABLE employees AS SELECT * FROM read_parquet('{dept_sql}')")
        count = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]
        print(f"Loaded employees: {count:,} rows")

        # Create tasks_with_dept: JOIN tasks to employees on normalized keys
        # Includes ALL columns from dept_mapping (employees)
        conn.execute("""
            CREATE TABLE tasks_with_dept AS
            SELECT
                t.*,
                e.userid AS emp_userid,
                e.id AS emp_id,
                e.full_name,
                e.title,
                e.netwarelogin,
                e.email,
                e.divisionid,
                e.division,
                e.division1,
                e.teamid,
                e.team
            FROM tasks t
            LEFT JOIN employees e
                ON LOWER(TRIM(t.operationby)) = LOWER(TRIM(e.userid))
        """)
        count = conn.execute("SELECT COUNT(*) FROM tasks_with_dept").fetchone()[0]
        print(f"Loaded tasks_with_dept: {count:,} rows")
    else:
        print(f"INFO: Dept mapping parquet not found (optional): {dept_parquet}")

    # Create analytical views
    print("\nCreating views...")

    # v_task_duration: all tasks with duration calculations
    conn.execute("""
        CREATE VIEW v_task_duration AS
        SELECT
            *,
            DATEDIFF('minute', starttime, endtime) AS duration_minutes,
            ROUND(DATEDIFF('minute', starttime, endtime) / 60.0, 2) AS duration_hours,
            ROUND(DATEDIFF('hour', dateinitiated, dateended), 2) AS lifecycle_hours
        FROM tasks
    """)
    print("  Created: v_task_duration")

    # v_daily_volume: tasks by date
    conn.execute("""
        CREATE VIEW v_daily_volume AS
        SELECT
            CAST(dateinitiated AS DATE) AS task_date,
            COUNT(*) AS tasks_initiated,
            SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN taskstatus = 'In Progress' THEN 1 ELSE 0 END) AS in_progress,
            SUM(CASE WHEN taskstatus = 'Pending' THEN 1 ELSE 0 END) AS pending
        FROM tasks
        WHERE dateinitiated IS NOT NULL
        GROUP BY CAST(dateinitiated AS DATE)
    """)
    print("  Created: v_daily_volume")

    # v_drawer_summary: group by drawer
    conn.execute("""
        CREATE VIEW v_drawer_summary AS
        SELECT
            drawer,
            COUNT(*) AS total_tasks,
            SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS completed,
            ROUND(AVG(DATEDIFF('minute', starttime, endtime) / 60.0), 2) AS avg_duration_hours
        FROM tasks
        GROUP BY drawer
    """)
    print("  Created: v_drawer_summary")

    # v_carrier_workload: group by carrier and flowname
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
    print("  Created: v_carrier_workload")

    # v_missing_status: tasks where taskstatus IS NULL
    conn.execute("""
        CREATE VIEW v_missing_status AS
        SELECT * FROM tasks WHERE taskstatus IS NULL
    """)
    print("  Created: v_missing_status")

    # v_tasks_by_department: LEFT JOIN tasks to employees (if employees table exists)
    tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
    if "employees" in tables:
        conn.execute("""
            CREATE VIEW v_tasks_by_department AS
            SELECT
                t.*,
                e.full_name,
                e.title,
                e.division,
                e.team
            FROM tasks t
            LEFT JOIN employees e ON LOWER(TRIM(t.assignedto)) = LOWER(TRIM(e.userid))
        """)
        print("  Created: v_tasks_by_department")

        # v_team_workload: INNER JOIN, group by team and division
        conn.execute("""
            CREATE VIEW v_team_workload AS
            SELECT
                e.team,
                e.division,
                COUNT(*) AS total_tasks,
                SUM(CASE WHEN t.taskstatus = 'Completed' THEN 1 ELSE 0 END) AS completed,
                ROUND(AVG(DATEDIFF('minute', t.starttime, t.endtime) / 60.0), 2) AS avg_hours
            FROM tasks t
            INNER JOIN employees e ON LOWER(TRIM(t.assignedto)) = LOWER(TRIM(e.userid))
            GROUP BY e.team, e.division
        """)
        print("  Created: v_team_workload")

    # Print summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)

    print("\nTables:")
    for row in conn.execute("SHOW TABLES").fetchall():
        table_name = row[0]
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  {table_name}: {count:,} rows")

    print("\nViews:")
    views = conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_type = 'VIEW'
    """).fetchall()
    for row in views:
        view_name = row[0]
        count = conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        print(f"  {view_name}: {count:,} rows")

    # File size
    conn.close()
    size_mb = duckdb_path.stat().st_size / (1024 * 1024)
    print(f"\nDatabase: {duckdb_path}")
    print(f"Size: {size_mb:.1f} MB")


if __name__ == "__main__":
    try:
        create_database()
        env = get_env()
        duckdb_path, _, _ = _paths_for_env(env)
        print("\nDone. Connect Power BI ODBC to:")
        print(f"  {duckdb_path}")
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
