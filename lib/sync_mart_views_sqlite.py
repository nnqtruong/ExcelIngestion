"""Translate dbt mart SQL (DuckDB dialect) to SQLite and create views on the shared warehouse."""
from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path

# ref() → SQLite relation names. Marts use stg_tasks view (synced from dbt staging) so
# aggregates match DuckDB (value maps + whitespace on drawer).
REF_TO_SQLITE_TABLE: dict[str, str] = {
    "stg_tasks": "stg_tasks",
    "stg_workers": "workers",
    "stg_employees": "employees",
    "stg_employees_master": "employees_master",
    "value_map_taskstatus": "value_map_taskstatus",
    "value_map_flowname": "value_map_flowname",
    "value_map_drawer": "value_map_drawer",
    "value_map_netwarelogin": "value_map_netwarelogin",
}

# dbt seed CSVs (same names as ref() targets for value maps).
SEEDS_DIR_NAME = "seeds"

# Staging models to materialize as SQLite views before marts (dependency order).
STAGING_VIEW_SQL_FILES = ("stg_tasks.sql",)

# Match {{ ref('name') }} with optional whitespace.
_REF_PATTERN = re.compile(
    r"\{\{\s*ref\s*\(\s*['\"]([a-zA-Z0-9_]+)['\"]\s*\)\s*\}\}",
    re.IGNORECASE,
)

# {{ source('raw', 'tasks') }} → tasks (SQLite export table).
_SOURCE_RAW_PATTERN = re.compile(
    r"\{\{\s*source\s*\(\s*['\"]raw['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}",
    re.IGNORECASE,
)
RAW_SOURCE_TO_TABLE = {
    "tasks": "tasks",
    "workers": "workers",
    "employees": "employees",
    "employees_master": "employees_master",
}


def strip_dbt_config(sql: str) -> str:
    """Remove leading {{ config(...) }} block."""
    sql = sql.strip()
    m = re.match(r"^\s*\{\{[\s\S]*?\}\}\s*", sql)
    if m:
        return sql[m.end() :].strip()
    return sql


def _collapse_whitespace_sql(expr: str) -> str:
    """Approximate DuckDB regexp_replace(expr, E'\\s+', ' ', 'g') without regexp extension."""
    e = expr.strip()
    inner = (
        f"replace(replace(replace(replace({e}, char(9), ' '), char(10), ' '), "
        f"char(13), ' '), char(160), ' ')"
    )
    for _ in range(16):
        inner = f"replace({inner}, '  ', ' ')"
    return f"trim({inner})"


def _substitute_raw_sources(sql: str) -> str:
    def _repl(m: re.Match) -> str:
        name = m.group(1)
        if name not in RAW_SOURCE_TO_TABLE:
            raise ValueError(f"Unsupported source('raw', {name!r}) — add to RAW_SOURCE_TO_TABLE")
        return RAW_SOURCE_TO_TABLE[name]

    return _SOURCE_RAW_PATTERN.sub(_repl, sql)


def translate_duckdb_mart_sql(sql: str) -> str:
    """Apply DuckDB → SQLite rules for marts that only use documented constructs."""
    sql = strip_dbt_config(sql)
    sql = _substitute_raw_sources(sql)

    # {{ ref('...') }} → table names (longer names first to avoid partial subs)
    for ref_name in sorted(REF_TO_SQLITE_TABLE, key=len, reverse=True):
        table = REF_TO_SQLITE_TABLE[ref_name]
        pat = re.compile(
            r"\{\{\s*ref\s*\(\s*['\"]" + re.escape(ref_name) + r"['\"]\s*\)\s*\}\}",
            re.IGNORECASE,
        )
        sql = pat.sub(table, sql)

    # Remaining ref() — substitute unknown refs with stg_* stripped prefix warning in comment only; fail safe
    def _ref_repl(m: re.Match) -> str:
        name = m.group(1)
        if name in REF_TO_SQLITE_TABLE:
            return REF_TO_SQLITE_TABLE[name]
        raise ValueError(f"Unsupported ref() in mart SQL: {name!r} — add to REF_TO_SQLITE_TABLE")

    sql = _REF_PATTERN.sub(_ref_repl, sql)

    # DuckDB regexp_replace on drawer (stg_tasks) — SQLite build may lack regexp_replace().
    # Source file uses E'\\s+' (two backslashes before s in the SQL file).
    sql = sql.replace(
        "regexp_replace(t.drawer, E'\\\\s+', ' ', 'g')",
        _collapse_whitespace_sql("t.drawer"),
    )

    # CAST(x AS VARCHAR) → CAST(x AS TEXT)
    sql = re.sub(r"(?i)\bas\s+varchar\b", "as TEXT", sql)

    # CAST(x AS DATE) → date(x). Use a simple inner expression only — avoid (.+?) which can
    # match "t.date" inside "t.dateinitiated" and mispair with the trailing " as date".
    sql = re.sub(
        r"(?is)cast\s*\(\s*([a-zA-Z0-9_.]+)\s+as\s+date\s*\)",
        lambda m: f"date({m.group(1).strip()})",
        sql,
    )

    # DATEDIFF must run before CURRENT_DATE → date('now'): nested parens in date('now')
    # break the datediff regex's third capture group (see mart_backlog avg_age_days).
    # DATEDIFF('unit', a, b) → julianday-based (b - a), same order as DuckDB
    def datediff_repl(m: re.Match) -> str:
        unit = m.group(1).lower()
        a = m.group(2).strip()
        b = m.group(3).strip()
        if unit == "minute":
            return f"((julianday({b}) - julianday({a})) * 1440)"
        if unit == "hour":
            return f"((julianday({b}) - julianday({a})) * 24)"
        if unit == "day":
            return f"(julianday({b}) - julianday({a}))"
        return m.group(0)

    sql = re.sub(
        r"(?is)datediff\s*\(\s*'(minute|hour|day)'\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)",
        datediff_repl,
        sql,
    )

    # CURRENT_DATE → date('now') (SQLite), after datediff so third arg stays simple
    sql = re.sub(r"(?i)\bCURRENT_DATE\b", "date('now')", sql)

    return sql


def load_mart_select_sql(mart_path: Path) -> str:
    raw = mart_path.read_text(encoding="utf-8")
    return translate_duckdb_mart_sql(raw)


def _view_body_starts_valid(body: str) -> bool:
    b = body.strip().lower()
    return b.startswith("select") or b.startswith("with")


def sync_seed_tables(
    db_path: Path,
    seeds_dir: Path,
    log: logging.Logger | None = None,
) -> None:
    """Load dbt seed CSVs into SQLite tables (names match dbt ref() for value maps)."""
    import pandas as pd

    logger = log or logging.getLogger(__name__)
    if not seeds_dir.is_dir():
        logger.warning("Seeds directory missing: %s", seeds_dir)
        return
    conn = sqlite3.connect(db_path)
    try:
        for csv_path in sorted(seeds_dir.glob("*.csv")):
            name = csv_path.stem
            df = pd.read_csv(csv_path)
            df.to_sql(name, conn, if_exists="replace", index=False)
            logger.info("Loaded seed table: %s (%d rows)", name, len(df))
        conn.commit()
    finally:
        conn.close()


def sync_staging_views(
    db_path: Path,
    staging_dir: Path,
    log: logging.Logger | None = None,
) -> tuple[list[str], list[str]]:
    """Create staging views (e.g. stg_tasks) before marts. Returns (created, errors)."""
    logger = log or logging.getLogger(__name__)
    created: list[str] = []
    errors: list[str] = []
    conn = sqlite3.connect(db_path)
    try:
        for fn in STAGING_VIEW_SQL_FILES:
            path = staging_dir / fn
            if not path.is_file():
                errors.append(f"{fn}: file not found")
                continue
            view_name = path.stem
            try:
                body = load_mart_select_sql(path)
                if not _view_body_starts_valid(body):
                    errors.append(f"{view_name}: expected SELECT or WITH after translation")
                    continue
                ddl = f'DROP VIEW IF EXISTS "{view_name}";\nCREATE VIEW "{view_name}" AS\n{body}'
                conn.executescript(ddl)
                conn.commit()
                created.append(view_name)
                logger.info("Created SQLite staging view: %s", view_name)
            except Exception as e:
                msg = f"{view_name}: {e}"
                errors.append(msg)
                logger.warning("Skipped staging view %s: %s", view_name, e)
    finally:
        conn.close()
    return created, errors


def sync_mart_views(
    db_path: Path,
    marts_dir: Path,
    log: logging.Logger | None = None,
) -> tuple[list[str], list[str]]:
    """
    Create or replace one SQLite view per mart SQL file in marts_dir.
    Returns (created_view_names, errors).
    """
    logger = log or logging.getLogger(__name__)
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")
    if not marts_dir.is_dir():
        raise FileNotFoundError(f"Marts directory not found: {marts_dir}")

    created: list[str] = []
    errors: list[str] = []

    sql_files = sorted(marts_dir.glob("*.sql"))
    conn = sqlite3.connect(db_path)
    try:
        for path in sql_files:
            view_name = path.stem
            try:
                body = load_mart_select_sql(path)
                if not _view_body_starts_valid(body):
                    errors.append(f"{view_name}: expected SELECT or WITH after translation")
                    continue
                ddl = f'DROP VIEW IF EXISTS "{view_name}";\nCREATE VIEW "{view_name}" AS\n{body}'
                conn.executescript(ddl)
                conn.commit()
                created.append(view_name)
                logger.info("Created SQLite view: %s", view_name)
            except Exception as e:
                msg = f"{view_name}: {e}"
                errors.append(msg)
                logger.warning("Skipped view %s: %s", view_name, e)
    finally:
        conn.close()

    return created, errors


def get_shared_warehouse_path(project_root: Path) -> Path:
    """analytics/dev_warehouse.db (dev) or analytics/warehouse.db (prod)."""
    import os

    env = os.environ.get("PIPELINE_ENV") or "dev"
    name = "dev_warehouse.db" if env == "dev" else "warehouse.db"
    return project_root / "analytics" / name
