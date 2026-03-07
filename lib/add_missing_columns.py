"""Step 03: Add missing schema columns with nulls (DuckDB)."""
import logging
from pathlib import Path

import duckdb
import psutil

from lib.schema import columns_as_list, get_column_order, load_schema
from lib.logging_util import monitor_step

SCHEMA_DTYPE_TO_DUCKDB = {
    "string": "VARCHAR",
    "int64": "BIGINT",
    "float64": "DOUBLE",
    "datetime64": "TIMESTAMP",
    "bool": "BOOLEAN",
}


def _escape_sql(s: str) -> str:
    return s.replace("'", "''")


def _quote_id(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _duckdb_type(schema_dtype: str) -> str:
    normalized = (schema_dtype or "string").strip().lower()
    return SCHEMA_DTYPE_TO_DUCKDB.get(normalized, "VARCHAR")


def process_file(path: Path, schema: dict, log: logging.Logger) -> None:
    """Add any schema columns missing from Parquet via DuckDB; reorder; overwrite."""
    proc = psutil.Process()
    rss_before_mb = proc.memory_info().rss / 1024 / 1024

    conn = duckdb.connect()
    input_path = path.resolve().as_posix()
    in_sql = _escape_sql(input_path)

    cur = conn.execute(f"SELECT * FROM read_parquet('{in_sql}') LIMIT 0")
    file_columns = [d[0] for d in cur.description]

    schema_cols = columns_as_list(schema)
    schema_order = get_column_order(schema)
    missing = [c["name"] for c in schema_cols if c["name"] not in file_columns]

    if not missing:
        conn.close()
        return

    # Build SELECT: schema order then extra columns; for missing use NULL::Type AS col
    output_order = [c for c in schema_order if c in file_columns or c in missing]
    extra = [c for c in file_columns if c not in schema_order]
    output_order = output_order + extra

    select_parts = []
    for col in output_order:
        if col in file_columns:
            select_parts.append(f'{_quote_id(col)}')
        else:
            dtype = _duckdb_type(next(c.get("dtype", "string") for c in schema_cols if c["name"] == col))
            select_parts.append(f"NULL::{dtype} AS {_quote_id(col)}")

    select_sql = ", ".join(select_parts)
    conn.execute(f"""
        COPY (
            SELECT {select_sql}
            FROM read_parquet('{in_sql}')
        ) TO '{in_sql}' (FORMAT PARQUET)
    """)
    conn.close()

    for col in missing:
        log.info("Added column %s to %s", col, path.name)

    rss_after_mb = proc.memory_info().rss / 1024 / 1024
    log.info("Add missing columns %s: memory %.1f -> %.1f MB", path.name, rss_before_mb, rss_after_mb)


@monitor_step
def run_add_missing_columns(clean_dir: Path, schema_path: Path, log: logging.Logger) -> int:
    """Process all Parquet in clean_dir via DuckDB. Returns number of files processed."""
    schema = load_schema(schema_path)
    if not clean_dir.is_dir():
        raise FileNotFoundError(f"No clean/ directory at {clean_dir}")
    parquet_files = sorted(clean_dir.glob("*.parquet"))
    parquet_files = [p for p in parquet_files if not p.name.endswith("_errors.parquet")]
    if not parquet_files:
        return 0
    for path in parquet_files:
        process_file(path, schema, log)
    return len(parquet_files)
