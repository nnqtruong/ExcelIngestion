"""
Build tasks mart Parquet: three-way LEFT JOIN (tasks + employees for assignedto, taskfrom, operationby)
via DuckDB. Writes datasets/tasks/analytics/tasks_mart.parquet. No pandas.
"""
import logging
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TASKS_PARQUET = ROOT / "datasets" / "tasks" / "analytics" / "combined.parquet"
DEPT_PARQUET = ROOT / "datasets" / "dept_mapping" / "analytics" / "combined.parquet"
MART_OUTPUT = ROOT / "datasets" / "tasks" / "analytics" / "tasks_mart.parquet"


def _escape_sql(s: str) -> str:
    return s.replace("'", "''")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger(__name__)

    if not TASKS_PARQUET.exists():
        log.error("Tasks Parquet not found: %s (run tasks pipeline through step 06 first)", TASKS_PARQUET)
        return 1
    if not DEPT_PARQUET.exists():
        log.error("Dept Parquet not found: %s (run dept_mapping pipeline through step 06 first)", DEPT_PARQUET)
        return 1

    tasks_path = _escape_sql(str(TASKS_PARQUET.resolve().as_posix()))
    dept_path = _escape_sql(str(DEPT_PARQUET.resolve().as_posix()))
    mart_path = _escape_sql(str(MART_OUTPUT.resolve().as_posix()))

    MART_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()

    conn.execute(f"""
        COPY (
            SELECT
                t.*,
                ea.full_name AS assigned_name,
                ea.title AS assigned_title,
                ea.division AS assigned_division,
                ea.divisionid AS assigned_divisionid,
                ea.team AS assigned_team,
                ea.email AS assigned_email,
                ef.full_name AS from_name,
                ef.title AS from_title,
                ef.division AS from_division,
                ef.team AS from_team,
                eo.full_name AS operation_name,
                eo.division AS operation_division,
                eo.team AS operation_team,
                ROUND((epoch(t.endtime) - epoch(t.starttime)) / 60.0, 2) AS duration_minutes,
                ROUND((epoch(t.endtime) - epoch(t.starttime)) / 3600.0, 2) AS duration_hours,
                ROUND((epoch(t.dateended) - epoch(t.dateinitiated)) / 3600.0, 2) AS lifecycle_hours,
                CAST(t.dateinitiated AS DATE) AS task_date
            FROM read_parquet('{tasks_path}') t
            LEFT JOIN read_parquet('{dept_path}') ea
                ON LOWER(TRIM(CAST(t.assignedto AS VARCHAR))) = LOWER(TRIM(ea.userid))
            LEFT JOIN read_parquet('{dept_path}') ef
                ON LOWER(TRIM(CAST(t.taskfrom AS VARCHAR))) = LOWER(TRIM(ef.userid))
            LEFT JOIN read_parquet('{dept_path}') eo
                ON LOWER(TRIM(CAST(t.operationby AS VARCHAR))) = LOWER(TRIM(eo.userid))
        ) TO '{mart_path}' (FORMAT PARQUET)
    """)

    task_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{tasks_path}')").fetchone()[0]
    mart_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{mart_path}')").fetchone()[0]

    if mart_count != task_count:
        log.error("Row count mismatch: tasks=%s, mart=%s", task_count, mart_count)
        conn.close()
        return 1

    # Optional: join match rates
    assigned_matched = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{tasks_path}') t
        INNER JOIN read_parquet('{dept_path}') e
            ON LOWER(TRIM(CAST(t.assignedto AS VARCHAR))) = LOWER(TRIM(e.userid))
    """).fetchone()[0]
    from_matched = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{tasks_path}') t
        INNER JOIN read_parquet('{dept_path}') e
            ON LOWER(TRIM(CAST(t.taskfrom AS VARCHAR))) = LOWER(TRIM(e.userid))
    """).fetchone()[0]
    op_matched = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{tasks_path}') t
        INNER JOIN read_parquet('{dept_path}') e
            ON LOWER(TRIM(CAST(t.operationby AS VARCHAR))) = LOWER(TRIM(e.userid))
    """).fetchone()[0]
    conn.close()

    log.info("Mart written: %s (%s rows)", MART_OUTPUT, mart_count)
    log.info("Join match rates: assignedto %.1f%%, taskfrom %.1f%%, operationby %.1f%%",
             (assigned_matched * 100.0 / task_count) if task_count else 0,
             (from_matched * 100.0 / task_count) if task_count else 0,
             (op_matched * 100.0 / task_count) if task_count else 0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
