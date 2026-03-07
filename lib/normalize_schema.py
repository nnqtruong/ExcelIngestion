"""Step 02: Normalize column names to snake_case and reorder to schema (DuckDB)."""
import logging
import re
from pathlib import Path

import duckdb
import psutil

from lib.schema import get_column_order, load_schema
from lib.logging_util import monitor_step


def to_snake_case(name: str) -> str:
    """Convert a string to lowercase snake_case."""
    s = re.sub(r"[^a-zA-Z0-9]+", "_", str(name).strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s.lower() if s else ""


def _escape_sql(s: str) -> str:
    """Escape single quotes for SQL string literal."""
    return s.replace("'", "''")


def _quote_id(name: str) -> str:
    """Double-quote identifier for DuckDB."""
    return '"' + name.replace('"', '""') + '"'


def process_file(path: Path, schema: dict, log: logging.Logger | None = None) -> None:
    """Normalize one Parquet file via DuckDB: rename to snake_case, reorder, overwrite."""
    logger = log or logging.getLogger(__name__)
    proc = psutil.Process()
    rss_before_mb = proc.memory_info().rss / 1024 / 1024

    conn = duckdb.connect()
    input_path = path.resolve().as_posix()
    in_sql = _escape_sql(input_path)

    # Get column names from Parquet
    cur = conn.execute(f"SELECT * FROM read_parquet('{in_sql}') LIMIT 0")
    original_columns = [d[0] for d in cur.description]

    rename_map = {c: to_snake_case(c) for c in original_columns}
    order = get_column_order(schema)
    ordered = [c for c in order if c in rename_map.values()]
    extra = [c for c in rename_map.values() if c not in order]
    output_cols = ordered + extra
    reverse_rename = {v: k for k, v in rename_map.items()}

    select_parts = [
        f'{_quote_id(reverse_rename[col])} AS {_quote_id(col)}'
        for col in output_cols
    ]
    select_sql = ", ".join(select_parts)

    conn.execute(f"""
        COPY (
            SELECT {select_sql}
            FROM read_parquet('{in_sql}')
        ) TO '{_escape_sql(input_path)}' (FORMAT PARQUET)
    """)
    conn.close()

    rss_after_mb = proc.memory_info().rss / 1024 / 1024
    logger.info("Normalize schema %s: memory %.1f -> %.1f MB", path.name, rss_before_mb, rss_after_mb)


@monitor_step
def run_normalize_schema(
    clean_dir: Path,
    schema_path: Path,
    log: logging.Logger | None = None,
) -> int:
    """Normalize all Parquet in clean_dir per schema via DuckDB. Returns number of files processed."""
    schema = load_schema(schema_path)
    if not clean_dir.is_dir():
        raise FileNotFoundError(f"No clean/ directory at {clean_dir}")
    parquet_files = sorted(clean_dir.glob("*.parquet"))
    parquet_files = [p for p in parquet_files if not p.name.endswith("_errors.parquet")]
    if not parquet_files:
        return 0
    for path in parquet_files:
        process_file(path, schema, log=log)
    return len(parquet_files)
