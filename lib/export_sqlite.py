"""Step 09: Export combined Parquet to SQLite with indexes."""
import logging
import sqlite3
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

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
    """Create indexes on commonly queried columns. Skips row_id (already primary).

    Auto-indexes: taskstatus, taskid, dateinitiated, userid, employee_id
    (only if column exists in the table).
    """
    # Columns that benefit from indexing for views and common queries
    auto_index_columns = {
        "taskstatus",  # v_missing_status, status breakdowns
        "taskid",  # unique task lookups
        "dateinitiated",  # v_daily_volume, date range queries
        "userid",  # employee joins
        "employee_id",  # employee lookups
        "assignedto",  # v_tasks_by_department join
        "operationby",  # v_tasks_by_department join
    }

    cursor = conn.cursor()
    columns_set = set(columns)
    indexed = []

    for col in auto_index_columns:
        if col in columns_set:
            idx_name = f"idx_{table_name}_{col}"
            try:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({col})")
                indexed.append(col)
            except sqlite3.OperationalError:
                pass

    conn.commit()
    if indexed:
        log.info("Created indexes on: %s", ", ".join(indexed))


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


def export_to_sqlite_chunked(
    parquet_path: Path,
    db_path: Path,
    table_name: str,
    log: logging.Logger,
    chunk_size: int = 100_000,
) -> dict:
    """Export Parquet to SQLite in chunks. Memory stays bounded by chunk size."""
    pf = pq.ParquetFile(parquet_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()

    first_chunk = True
    total_rows = 0
    columns: list[str] | None = None

    for batch in pf.iter_batches(batch_size=chunk_size):
        df_chunk = batch.to_pandas()
        df_chunk = prepare_dataframe_for_sqlite(df_chunk)

        if first_chunk:
            columns = list(df_chunk.columns)
            log.info(
                "Starting chunked export to %s (chunk_size=%d)",
                table_name,
                chunk_size,
            )

        df_chunk.to_sql(
            table_name,
            conn,
            if_exists="append",
            index=False,
        )

        total_rows += len(df_chunk)
        first_chunk = False

        if total_rows % 500_000 == 0 and total_rows > 0:
            log.info("  Written %d rows...", total_rows)

    if columns is None:
        columns = list(pf.schema_arrow.names)
        if total_rows == 0:
            pd.DataFrame(columns=columns).to_sql(
                table_name, conn, if_exists="append", index=False
            )

    log.info("Wrote %d rows to %s table in %s", total_rows, table_name, db_path.name)

    if columns:
        create_indexes(conn, table_name, columns, log)

    results = run_verification_queries(conn, table_name, columns or [], log)
    conn.close()
    return results


def export_to_sqlite(
    parquet_path: Path,
    db_path: Path,
    table_name: str,
    log: logging.Logger,
) -> dict:
    """Export Parquet to SQLite. Create-or-replace only this table; do not drop other tables.

    Large Parquet files use PyArrow batched reads so the full file is not loaded at once.
    """
    file_size_mb = parquet_path.stat().st_size / (1024 * 1024)

    if file_size_mb > 50:
        log.info("Large file (%.1fMB), using chunked export", file_size_mb)
        return export_to_sqlite_chunked(parquet_path, db_path, table_name, log)

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
