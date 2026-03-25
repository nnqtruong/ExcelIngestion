"""Step 10: Create SQLite analytics views by syncing dbt mart SQL (translated to SQLite)."""
import logging
import sqlite3
from pathlib import Path

from lib.logging_util import monitor_step
from lib.paths import ROOT
from lib.sync_mart_views_sqlite import (
    SEEDS_DIR_NAME,
    sync_mart_views,
    sync_seed_tables,
    sync_staging_views,
)

# dbt mart models → one SQLite view each (same name as file stem).
MARTS_DIR = ROOT / "dbt_crc" / "models" / "marts"
STAGING_DIR = ROOT / "dbt_crc" / "models" / "staging"
SEEDS_DIR = ROOT / "dbt_crc" / SEEDS_DIR_NAME

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

# Marts that join tasks to workers: verify with LIMIT 1 instead of full COUNT(*).
# These are slow on SQLite with millions of task rows.
_SLOW_VERIFY_VIEWS = frozenset({
    "mart_tasks_enriched",
    "mart_team_demand",
    "mart_turnaround",
})


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


def verify_views(conn: sqlite3.Connection, view_names: list[str], log: logging.Logger) -> dict:
    """Verify views are queryable; return {view_name: row_count or sentinel}."""
    cursor = conn.cursor()
    results: dict[str, int] = {}
    for view_name in view_names:
        try:
            if view_name in _SLOW_VERIFY_VIEWS:
                cursor.execute(f'SELECT 1 FROM "{view_name}" LIMIT 1')
                row = cursor.fetchone()
                results[view_name] = 1 if row else 0
                log.info("View %s: verified (skipped full count for performance)", view_name)
            else:
                cursor.execute(f'SELECT COUNT(*) FROM "{view_name}"')
                results[view_name] = cursor.fetchone()[0]
                log.info("View %s: %d rows", view_name, results[view_name])
        except sqlite3.Error as e:
            log.warning("Failed to query view %s: %s", view_name, e)
            results[view_name] = -1
    return results


@monitor_step
def run_sqlite_views(db_path: Path, log: logging.Logger) -> dict:
    """Drop legacy v_* views, sync dbt marts as SQLite views, verify. Returns verify results."""
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        _drop_legacy_views(conn, log)
    finally:
        conn.close()

    sync_seed_tables(db_path, SEEDS_DIR, log)

    if not STAGING_DIR.is_dir():
        log.warning("Staging directory missing (%s); no staging views.", STAGING_DIR)
    else:
        staging_created, staging_errors = sync_staging_views(db_path, STAGING_DIR, log)
        for err in staging_errors:
            log.warning("Staging view sync: %s", err)
        if not staging_created:
            log.warning("No staging views created; marts may not match DuckDB.")

    if not MARTS_DIR.is_dir():
        log.warning("Marts directory missing (%s); no mart views created.", MARTS_DIR)
        return {}

    created, errors = sync_mart_views(db_path, MARTS_DIR, log)
    for err in errors:
        log.warning("Mart view sync: %s", err)

    if not created:
        return {}

    conn = sqlite3.connect(db_path)
    try:
        return verify_views(conn, created, log)
    finally:
        conn.close()
