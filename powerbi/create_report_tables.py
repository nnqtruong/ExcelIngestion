"""Verify dbt-built DuckDB warehouse tables/views exist and are populated.

Dbt is the source of truth (``dbt run`` materializes staging + marts into DuckDB).
This script does not create ``report_*`` or other tables — it only checks the
shared warehouse file used by Power BI ODBC.

Run after ``dbt run`` (and pipeline Parquet refresh). Uses ``PIPELINE_ENV``
(default ``dev``): ``powerbi/dev_warehouse.duckdb`` vs ``powerbi/warehouse.duckdb``.
"""
from __future__ import annotations

import contextlib
import os
import sys
from pathlib import Path
from typing import Iterator

import duckdb

ROOT = Path(__file__).resolve().parent.parent
POWERBI_DIR = ROOT / "powerbi"
# Staging views reference parquet via paths relative to ``dbt_crc`` (see ``sources.yml``).
DBT_PROJECT_DIR = ROOT / "dbt_crc"


@contextlib.contextmanager
def _cwd(path: Path) -> Iterator[None]:
    prev = Path.cwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(prev)


def get_env() -> str:
    return os.environ.get("PIPELINE_ENV") or "dev"


def get_duckdb_path() -> Path:
    env = get_env()
    name = "dev_warehouse.duckdb" if env == "dev" else "warehouse.duckdb"
    return POWERBI_DIR / name


# Objects dbt should create (staging views + mart tables) — keep in sync with dbt_crc/models.
EXPECTED_RELATIONS = (
    "stg_tasks",
    "stg_employees",
    "stg_workers",
    "stg_employees_master",
    "mart_tasks_enriched",
    "mart_team_capacity",
    "mart_team_demand",
    "mart_onshore_offshore",
    "mart_backlog",
    "mart_turnaround",
    "mart_daily_trend",
)

# Must have at least one row (core fact / dimensions loaded from pipeline).
REQUIRE_NONEMPTY = frozenset(
    {
        "stg_tasks",
        "stg_workers",
        "stg_employees_master",
        "mart_tasks_enriched",
    }
)


def list_main_relations(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Return {name: table_type} for ``main`` schema."""
    rows = conn.execute(
        """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
        """
    ).fetchall()
    return {name: typ for name, typ in rows}


def verify_dbt_warehouse(db_path: Path) -> tuple[bool, list[str]]:
    """
    Return (ok, messages). ok is False if any expected relation is missing,
    or if a REQUIRE_NONEMPTY relation has zero rows.
    """
    messages: list[str] = []
    if not db_path.exists():
        messages.append(f"FAIL: DuckDB file not found: {db_path}")
        messages.append("  Run: cd dbt_crc && dbt run")
        return False, messages

    if not DBT_PROJECT_DIR.is_dir():
        messages.append(f"FAIL: dbt project directory not found: {DBT_PROJECT_DIR}")
        return False, messages

    # Views read ``../datasets/...`` relative to ``dbt_crc`` (same cwd as ``dbt run``).
    with _cwd(DBT_PROJECT_DIR):
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            existing = list_main_relations(conn)
            ok = True

            for name in EXPECTED_RELATIONS:
                if name not in existing:
                    messages.append(f"FAIL: missing relation: {name}")
                    ok = False
                    continue

                count = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
                typ = existing[name]
                line = f"  OK {name} ({typ}): {count:,} rows"
                if name in REQUIRE_NONEMPTY and count == 0:
                    messages.append(f"FAIL: {name} is empty (expected rows)")
                    ok = False
                elif count == 0:
                    messages.append(f"{line}  (warning: empty aggregate or staging)")
                else:
                    messages.append(line)

            extras = sorted(set(existing) - set(EXPECTED_RELATIONS))
            if extras:
                messages.append(f"  (other objects in main: {', '.join(extras)})")
        finally:
            conn.close()

    if ok:
        messages.insert(0, f"PASS: dbt warehouse verified - {db_path}")
    return ok, messages


def main() -> int:
    db_path = get_duckdb_path()
    print(f"Environment: {get_env()}")
    print(f"DuckDB: {db_path}\n")

    ok, messages = verify_dbt_warehouse(db_path)
    for line in messages:
        print(line)

    if not ok:
        print("\nFix: run pipeline Parquet builds, then `cd dbt_crc && dbt run`", file=sys.stderr)
        return 1
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
