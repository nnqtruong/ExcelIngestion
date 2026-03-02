"""Step 09: Export combined Parquet to SQLite database with indexes."""

import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import yaml

# Project root (parent of scripts/)
ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
ANALYTICS_DIR = ROOT / "analytics"
LOGS_DIR = ROOT / "logs"
COMBINE_PATH = CONFIG_DIR / "combine.yaml"
DB_PATH = ANALYTICS_DIR / "tasks.db"


def setup_logging() -> None:
    """Configure logging to pipeline.log and console."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / "pipeline.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def get_combined_path() -> Path:
    """Return path to combined Parquet in analytics/ (from combine.yaml or default)."""
    if COMBINE_PATH.exists():
        with open(COMBINE_PATH, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        if config and config.get("output"):
            return ANALYTICS_DIR / config["output"]
    return ANALYTICS_DIR / "combined.parquet"


def prepare_dataframe_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime columns to ISO 8601 strings for SQLite compatibility."""
    df = df.copy()
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def create_indexes(conn: sqlite3.Connection, columns: list[str], log: logging.Logger) -> None:
    """Create indexes on frequently queried columns (if they exist in data)."""
    index_columns = [
        "taskid",
        "taskstatus",
        "drawer",
        "carrier",
        "flowname",
        "effectivedate",
        "dateinitiated",
        "dateended",
    ]
    cursor = conn.cursor()
    for col in index_columns:
        if col in columns:
            idx_name = f"idx_tasks_{col}"
            sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON tasks ({col})"
            cursor.execute(sql)
            log.info("Created index %s on %s", idx_name, col)
    conn.commit()


def run_verification_queries(conn: sqlite3.Connection, log: logging.Logger) -> dict:
    """Run verification queries and log results."""
    cursor = conn.cursor()
    results = {}

    # Total rows
    cursor.execute("SELECT COUNT(*) FROM tasks")
    total_rows = cursor.fetchone()[0]
    results["total_rows"] = total_rows
    log.info("SQLite verification: total rows = %d", total_rows)

    # Unique tasks (taskid may have duplicates across months)
    cursor.execute("SELECT COUNT(DISTINCT taskid) FROM tasks")
    unique_tasks = cursor.fetchone()[0]
    results["unique_tasks"] = unique_tasks
    log.info("SQLite verification: unique taskid = %d", unique_tasks)

    # Status breakdown
    cursor.execute("SELECT taskstatus, COUNT(*) FROM tasks GROUP BY taskstatus")
    status_breakdown = cursor.fetchall()
    results["status_breakdown"] = status_breakdown
    log.info("SQLite verification: status breakdown:")
    for status, count in status_breakdown:
        log.info("  %s: %d", status if status else "(NULL)", count)

    return results


def export_to_sqlite(parquet_path: Path, db_path: Path, log: logging.Logger) -> dict:
    """Export Parquet to SQLite database. row_id is the primary key."""
    # Read parquet
    df = pd.read_parquet(parquet_path)
    log.info("Read %d rows from %s", len(df), parquet_path.name)

    # Prepare data for SQLite
    df = prepare_dataframe_for_sqlite(df)

    # Remove existing database
    if db_path.exists():
        try:
            db_path.unlink()
            log.info("Removed existing database: %s", db_path.name)
        except PermissionError:
            log.error(
                "Cannot delete %s - file is locked. Close any applications using the database.",
                db_path.name,
            )
            sys.exit(1)

    # Create connection and write
    conn = sqlite3.connect(db_path)
    df.to_sql("tasks", conn, if_exists="replace", index=False)
    log.info("Wrote %d rows to tasks table in %s", len(df), db_path.name)

    # Create indexes
    create_indexes(conn, list(df.columns), log)

    # Run verification queries
    results = run_verification_queries(conn, log)

    conn.close()
    return results


def main() -> None:
    setup_logging()
    log = logging.getLogger(__name__)

    parquet_path = get_combined_path()
    if not parquet_path.exists():
        log.error("Combined Parquet not found: %s (run step 06 first)", parquet_path)
        sys.exit(1)

    results = export_to_sqlite(parquet_path, DB_PATH, log)

    # Print summary
    print(f"\nSQLite Export Summary:")
    print(f"  Database: {DB_PATH}")
    print(f"  Total rows: {results['total_rows']}")
    print(f"  Unique taskids: {results['unique_tasks']}")
    print(f"\n  Status breakdown:")
    for status, count in results["status_breakdown"]:
        print(f"    {status if status else '(NULL)'}: {count}")

    log.info("Finished SQLite export to %s", DB_PATH.name)


if __name__ == "__main__":
    main()
