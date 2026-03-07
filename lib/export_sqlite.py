"""Step 09: Export combined Parquet to SQLite with indexes."""
import logging
import sqlite3
from pathlib import Path

import pandas as pd

from lib.logging_util import monitor_step


def prepare_dataframe_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime columns to ISO 8601 strings for SQLite."""
    df = df.copy()
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def create_indexes(
    conn: sqlite3.Connection, table_name: str, columns: list[str], log: logging.Logger
) -> None:
    """Create indexes on columns that exist. Skips row_id (already primary)."""
    cursor = conn.cursor()
    for col in columns:
        if col == "row_id":
            continue
        idx_name = f"idx_{table_name}_{col}"
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({col})")
            log.info("Created index %s on %s", idx_name, col)
        except sqlite3.OperationalError:
            pass
    conn.commit()


def run_verification_queries(
    conn: sqlite3.Connection, table_name: str, columns: list[str], log: logging.Logger
) -> dict:
    """Run verification queries; only use columns that exist (dataset-agnostic)."""
    cursor = conn.cursor()
    results = {"total_rows": None}
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    results["total_rows"] = cursor.fetchone()[0]
    log.info("SQLite verification: total rows = %d", results["total_rows"])
    if "taskid" in columns:
        cursor.execute(f"SELECT COUNT(DISTINCT taskid) FROM {table_name}")
        results["unique_tasks"] = cursor.fetchone()[0]
        log.info("SQLite verification: unique taskid = %d", results["unique_tasks"])
    if "taskstatus" in columns:
        cursor.execute(f"SELECT taskstatus, COUNT(*) FROM {table_name} GROUP BY taskstatus")
        results["status_breakdown"] = cursor.fetchall()
        for status, count in results["status_breakdown"]:
            log.info("  %s: %d", status if status else "(NULL)", count)
    return results


def export_to_sqlite(
    parquet_path: Path,
    db_path: Path,
    table_name: str,
    log: logging.Logger,
) -> dict:
    """Export Parquet to SQLite. Create-or-replace only this table; do not drop other tables."""
    df = pd.read_parquet(parquet_path)
    log.info("Read %d rows from %s", len(df), parquet_path.name)
    df = prepare_dataframe_for_sqlite(df)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    df.to_sql(table_name, conn, if_exists="append", index=False)
    log.info("Wrote %d rows to %s table in %s", len(df), table_name, db_path.name)
    columns = list(df.columns)
    create_indexes(conn, table_name, columns, log)
    results = run_verification_queries(conn, table_name, columns, log)
    conn.close()
    return results


@monitor_step
def run_export_sqlite(
    combined_path: Path,
    db_path: Path,
    table_name: str,
    log: logging.Logger,
) -> dict:
    """Export combined Parquet to SQLite. Returns verification results."""
    if not combined_path.exists():
        raise FileNotFoundError(f"Combined Parquet not found: {combined_path}")
    return export_to_sqlite(combined_path, db_path, table_name, log)
