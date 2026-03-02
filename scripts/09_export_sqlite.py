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


def get_surrogate_key_name() -> str | None:
    """Get surrogate key name from combine.yaml if configured."""
    if COMBINE_PATH.exists():
        with open(COMBINE_PATH, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        if config and config.get("add_surrogate_key"):
            return config.get("surrogate_key_name", "row_id")
    return None


def get_sqlite_dtype(col_name: str, pandas_dtype: str, surrogate_key: str | None) -> str:
    """Map column to SQLite type based on name and pandas dtype."""
    # Surrogate primary key (if configured)
    if surrogate_key and col_name == surrogate_key:
        return "INTEGER PRIMARY KEY"
    # Integer columns
    if col_name in ("taskid", "filenumber"):
        return "INTEGER"
    # Date/time columns -> TEXT (ISO 8601 format)
    if "date" in col_name or "time" in col_name:
        return "TEXT"
    # Everything else -> TEXT
    return "TEXT"


def prepare_dataframe_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime columns to ISO 8601 strings for SQLite compatibility."""
    df = df.copy()
    for col in df.columns:
        if "date" in col or "time" in col:
            # Convert datetime to ISO 8601 string
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def create_indexes(conn: sqlite3.Connection, log: logging.Logger) -> None:
    """Create indexes on frequently queried columns."""
    indexes = [
        ("idx_tasks_taskid", "taskid"),  # Index on taskid for fast lookups
        ("idx_tasks_status", "taskstatus"),
        ("idx_tasks_drawer", "drawer"),
        ("idx_tasks_carrier", "carrier"),
        ("idx_tasks_flowname", "flowname"),
        ("idx_tasks_effectivedate", "effectivedate"),
        ("idx_tasks_dateinitiated", "dateinitiated"),
        ("idx_tasks_dateended", "dateended"),  # For time-based queries
    ]
    cursor = conn.cursor()
    for idx_name, col_name in indexes:
        sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON tasks ({col_name})"
        cursor.execute(sql)
        log.info("Created index %s on %s", idx_name, col_name)
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

    # Unique tasks
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

    # Flow breakdown
    cursor.execute("SELECT flowname, COUNT(*) FROM tasks GROUP BY flowname")
    flow_breakdown = cursor.fetchall()
    results["flow_breakdown"] = flow_breakdown
    log.info("SQLite verification: flowname breakdown:")
    for flow, count in flow_breakdown:
        log.info("  %s: %d", flow if flow else "(NULL)", count)

    return results


def export_to_sqlite(parquet_path: Path, db_path: Path, log: logging.Logger) -> dict:
    """Export Parquet to SQLite database."""
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

    # Create connection
    conn = sqlite3.connect(db_path)

    # Write to SQLite
    df.to_sql("tasks", conn, if_exists="replace", index=False)
    log.info("Wrote %d rows to tasks table in %s", len(df), db_path.name)

    # Create indexes
    create_indexes(conn, log)

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
    print(f"  Unique tasks: {results['unique_tasks']}")
    print(f"\n  Status breakdown:")
    for status, count in results["status_breakdown"]:
        print(f"    {status if status else '(NULL)'}: {count}")
    print(f"\n  Flowname breakdown:")
    for flow, count in results["flow_breakdown"]:
        print(f"    {flow if flow else '(NULL)'}: {count}")

    log.info("Finished SQLite export to %s", DB_PATH.name)


if __name__ == "__main__":
    main()
