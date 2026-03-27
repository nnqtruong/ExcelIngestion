"""Step 10: SQLite analytics warehouse — seeds, staging view, materialized _stg_tasks_enriched, native mart views."""
import logging
import sqlite3
import time
from pathlib import Path

from lib.logging_util import monitor_step
from lib.paths import ROOT
from lib.sync_mart_views_sqlite import SEEDS_DIR_NAME, sync_seed_tables, sync_staging_views

# dbt mart models → one SQLite view each (same name as file stem).
MARTS_DIR = ROOT / "dbt_crc" / "models" / "marts"
STAGING_DIR = ROOT / "dbt_crc" / "models" / "staging"
SEEDS_DIR = ROOT / "dbt_crc" / SEEDS_DIR_NAME

# Manifesto Step 2: indexes for mart GROUP BY / filter columns on _stg_tasks_enriched.
_STG_TASKS_ENRICHED_INDEXES_SQL = """
CREATE INDEX idx_stg_task_date ON _stg_tasks_enriched(task_date);
CREATE INDEX idx_stg_drawer ON _stg_tasks_enriched(drawer);
CREATE INDEX idx_stg_taskstatus ON _stg_tasks_enriched(taskstatus);
CREATE INDEX idx_stg_flowname ON _stg_tasks_enriched(flowname);
CREATE INDEX idx_stg_cost_center ON _stg_tasks_enriched(cost_center_hierarchy);
CREATE INDEX idx_stg_source ON _stg_tasks_enriched(employee_source);
CREATE INDEX idx_stg_stepname ON _stg_tasks_enriched(stepname);
"""

# Manifesto Step 3: mart views read from _stg_tasks_enriched (no re-join via translated dbt SQL).
SQLITE_MART_VIEWS: dict[str, str] = {
    "mart_tasks_enriched": """
        SELECT * FROM _stg_tasks_enriched
    """,
    "mart_team_capacity": """
        SELECT
            cost_center_hierarchy AS department,
            cost_center,
            management_level,
            COUNT(*) AS headcount,
            SUM(fte) AS total_fte,
            SUM(scheduled_weekly_hours) AS total_weekly_hours
        FROM workers
        WHERE current_status = 'Active'
        GROUP BY cost_center_hierarchy, cost_center, management_level
    """,
    "mart_team_demand": """
        SELECT
            cost_center_hierarchy AS department,
            cost_center,
            task_date AS task_week,
            COUNT(*) AS task_count,
            SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS completed,
            ROUND(AVG(duration_hours), 2) AS avg_handle_hours
        FROM _stg_tasks_enriched
        WHERE duration_hours IS NOT NULL
        GROUP BY cost_center_hierarchy, cost_center, task_date
    """,
    "mart_onshore_offshore": """
        SELECT
            employee_source AS source_system,
            flowname,
            stepname,
            COUNT(*) AS task_count,
            ROUND(AVG(duration_hours), 2) AS avg_handle_hours,
            ROUND(
                SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                1
            ) AS completion_rate
        FROM _stg_tasks_enriched
        WHERE duration_hours IS NOT NULL
        GROUP BY employee_source, flowname, stepname
    """,
    "mart_backlog": """
        SELECT
            drawer,
            flowname,
            stepname,
            taskstatus,
            COUNT(*) AS task_count,
            ROUND(AVG(julianday('now') - julianday(dateinitiated)), 1) AS avg_age_days
        FROM _stg_tasks_enriched
        WHERE taskstatus IS NOT NULL AND taskstatus != 'Completed'
        GROUP BY drawer, flowname, stepname, taskstatus
    """,
    "mart_turnaround": """
        SELECT
            drawer,
            flowname,
            stepname,
            COUNT(*) AS completed_count,
            ROUND(AVG(duration_hours), 2) AS avg_handle_hours,
            ROUND(AVG(lifecycle_hours), 2) AS avg_lifecycle_hours
        FROM _stg_tasks_enriched
        WHERE taskstatus = 'Completed'
            AND duration_hours IS NOT NULL
        GROUP BY drawer, flowname, stepname
    """,
    "mart_daily_trend": """
        SELECT
            task_date,
            drawer,
            COUNT(*) AS tasks_opened,
            SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS tasks_completed,
            COUNT(*) - SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS net_backlog_change
        FROM _stg_tasks_enriched
        GROUP BY task_date, drawer
    """,
}


# Legacy v_* views removed in analytics restructure; drop if still present.
LEGACY_VIEWS_TO_DROP = (
    "v_task_duration",
    "v_daily_volume",
    "v_drawer_summary",
    "v_carrier_workload",
    "v_missing_status",
    "v_tasks_by_department",
    "v_tasks_with_workers",
)

def expected_mart_view_names() -> list[str]:
    """Names of mart views that sync should create (from MARTS_DIR/*.sql)."""
    if not MARTS_DIR.is_dir():
        return []
    return sorted(p.stem for p in MARTS_DIR.glob("*.sql"))


def _drop_legacy_views(conn: sqlite3.Connection, log: logging.Logger) -> None:
    cursor = conn.cursor()
    for name in LEGACY_VIEWS_TO_DROP:
        try:
            cursor.execute(f'DROP VIEW IF EXISTS "{name}"')
            log.info("Dropped legacy view: %s", name)
        except sqlite3.Error as e:
            log.warning("Could not drop legacy view %s: %s", name, e)
    conn.commit()


def create_materialized_staging_table(db_path: Path, log: logging.Logger) -> None:
    """
    Build _stg_tasks_enriched: one pass joining stg_tasks + workers + employees_master.

    Runs after seeds and the stg_tasks view exist; before mart views (see manifesto Step 1).
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')")
        existing_relations = {row[0] for row in cursor.fetchall()}

        has_stg_tasks = "stg_tasks" in existing_relations
        has_tasks = "tasks" in existing_relations
        if not has_stg_tasks and not has_tasks:
            log.warning("tasks/stg_tasks not found; skipping _stg_tasks_enriched creation")
            return

        has_workers = "workers" in existing_relations
        has_employees_master = "employees_master" in existing_relations
        log.info(
            "Creating _stg_tasks_enriched (workers=%s, employees_master=%s)",
            has_workers,
            has_employees_master,
        )

        worker_cols = """
    NULL AS teammate,
    NULL AS worker_job_profile,
    NULL AS business_title,
    NULL AS management_level,
    NULL AS cost_center,
    NULL AS cost_center_hierarchy,
    NULL AS fte,
    NULL AS scheduled_weekly_hours,
    NULL AS direct_manager,
    NULL AS worker_status,"""
        worker_join = ""
        if has_workers:
            worker_cols = """
    w.teammate,
    w.job_profile AS worker_job_profile,
    w.business_title,
    w.management_level,
    w.cost_center,
    w.cost_center_hierarchy,
    w.fte,
    w.scheduled_weekly_hours,
    w.direct_manager,
    w.current_status AS worker_status,"""
            worker_join = """
LEFT JOIN workers w
    ON t.assignedto = LOWER(TRIM(CAST(w.employee_id AS TEXT)))"""

        employee_cols = """
    NULL AS employee_source,
    NULL AS employee_master_name,"""
        employee_join = ""
        if has_employees_master:
            employee_cols = """
    em.source_system AS employee_source,
    em.name AS employee_master_name,"""
            employee_join = """
LEFT JOIN employees_master em
    ON t.assignedto = LOWER(TRIM(CAST(em.employee_id AS TEXT)))"""

        create_sql = f"""
DROP TABLE IF EXISTS _stg_tasks_enriched;

CREATE TABLE _stg_tasks_enriched AS
SELECT
    t.*,{worker_cols}{employee_cols}
    (julianday(t.endtime) - julianday(t.starttime)) * 1440 AS duration_minutes,
    ROUND((julianday(t.endtime) - julianday(t.starttime)) * 24, 2) AS duration_hours,
    ROUND((julianday(t.dateended) - julianday(t.dateinitiated)) * 24, 2) AS lifecycle_hours,
    DATE(t.dateinitiated) AS task_date
FROM stg_tasks t{worker_join}{employee_join};
"""

        t0 = time.perf_counter()
        cursor.executescript(create_sql)
        conn.commit()
        elapsed = time.perf_counter() - t0
        cursor.execute("SELECT COUNT(*) FROM _stg_tasks_enriched")
        row_count = cursor.fetchone()[0]
        log.info(
            "Materialized _stg_tasks_enriched: %d rows in %.2fs",
            row_count,
            elapsed,
        )
    finally:
        conn.close()


def index_materialized_staging_table(db_path: Path, log: logging.Logger) -> None:
    """Create indexes on _stg_tasks_enriched (manifesto Step 2). Table must already exist."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_stg_tasks_enriched' LIMIT 1"
        )
        if cursor.fetchone() is None:
            log.warning("_stg_tasks_enriched not found; skipping index creation")
            return
        t0 = time.perf_counter()
        cursor.executescript(_STG_TASKS_ENRICHED_INDEXES_SQL)
        conn.commit()
        elapsed = time.perf_counter() - t0
        log.info("Indexed _stg_tasks_enriched in %.2fs", elapsed)
    finally:
        conn.close()


def create_sqlite_mart_views(db_path: Path, log: logging.Logger) -> list[str]:
    """Create mart views from SQLITE_MART_VIEWS (requires _stg_tasks_enriched). Returns created view names."""
    created: list[str] = []
    conn = sqlite3.connect(db_path)
    try:
        for view_name in sorted(SQLITE_MART_VIEWS):
            body = SQLITE_MART_VIEWS[view_name].strip()
            try:
                ddl = f'DROP VIEW IF EXISTS "{view_name}";\nCREATE VIEW "{view_name}" AS\n{body}'
                conn.executescript(ddl)
                conn.commit()
                created.append(view_name)
                log.info("Created SQLite mart view: %s", view_name)
            except Exception as e:
                log.warning("Skipped mart view %s: %s", view_name, e)
    finally:
        conn.close()
    return created


def verify_views(conn: sqlite3.Connection, view_names: list[str], log: logging.Logger) -> dict:
    """Verify each view exists with LIMIT 1 check (avoids full scan). Returns {view_name: 1 (exists) or -1 (failed)}."""
    cursor = conn.cursor()
    results: dict[str, int] = {}
    for view_name in view_names:
        try:
            conn.execute("PRAGMA busy_timeout = 5000")
            cursor.execute(f'SELECT 1 FROM "{view_name}" LIMIT 1')
            cursor.fetchone()
            results[view_name] = 1
            log.info("View %s: exists", view_name)
        except sqlite3.Error as e:
            log.warning("Failed to query view %s: %s", view_name, e)
            results[view_name] = -1
    return results


@monitor_step
def run_sqlite_views(db_path: Path, log: logging.Logger) -> dict:
    """Drop legacy views, create materialized staging, create mart views, verify."""
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        _drop_legacy_views(conn, log)
    finally:
        conn.close()

    sync_seed_tables(db_path, SEEDS_DIR, log)

    if STAGING_DIR.is_dir():
        _, staging_errors = sync_staging_views(db_path, STAGING_DIR, log)
        for err in staging_errors:
            log.warning("Staging view sync: %s", err)

    create_materialized_staging_table(db_path, log)
    index_materialized_staging_table(db_path, log)

    conn = sqlite3.connect(db_path)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_stg_tasks_enriched' LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    if not exists:
        log.warning("_stg_tasks_enriched is missing; skipping mart view creation")
        return {}

    created = create_sqlite_mart_views(db_path, log)

    if not created:
        return {}

    conn = sqlite3.connect(db_path)
    try:
        return verify_views(conn, created, log)
    finally:
        conn.close()
