"""Step 02: Normalize column names to snake_case and reorder to schema (DuckDB)."""
import logging
import re
from pathlib import Path

import duckdb
import psutil

from lib.schema import get_column_aliases, get_column_order, load_schema
from lib.sql_utils import escape_sql_string, quote_identifier
from lib.logging_util import monitor_step


def to_snake_case(name: str) -> str:
    """Convert a string to lowercase snake_case."""
    s = re.sub(r"[^a-zA-Z0-9]+", "_", str(name).strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s.lower() if s else ""


def process_file(path: Path, schema: dict, log: logging.Logger | None = None) -> None:
    """Normalize one Parquet file via DuckDB: rename to snake_case, reorder, overwrite."""
    logger = log or logging.getLogger(__name__)
    proc = psutil.Process()
    rss_before_mb = proc.memory_info().rss / 1024 / 1024

    conn = duckdb.connect()
    input_path = path.resolve().as_posix()
    in_sql = escape_sql_string(input_path)

    # Get column names from Parquet
    cur = conn.execute(f"SELECT * FROM read_parquet('{in_sql}') LIMIT 0")
    original_columns = [d[0] for d in cur.description]

    aliases = get_column_aliases(schema)
    rename_map: dict[str, str] = {}
    for c in original_columns:
        snake = to_snake_case(c)
        if c in aliases:
            rename_map[c] = aliases[c]
        elif snake in aliases:
            rename_map[c] = aliases[snake]
        else:
            rename_map[c] = snake

    finals_present = set(rename_map.values())
    order = get_column_order(schema)
    # Only keep columns that are in column_order - drop extras not in schema
    ordered = [c for c in order if c in finals_present]

    # Log dropped columns for visibility
    extra_cols = [f for f in finals_present if f not in set(order)]
    if extra_cols:
        logger.info("Dropping columns not in schema: %s", extra_cols)

    output_cols = ordered

    def _original_for_final(final: str) -> str:
        for orig, fn in rename_map.items():
            if fn == final:
                return orig
        raise KeyError(final)

    select_parts = [
        f'{quote_identifier(_original_for_final(col))} AS {quote_identifier(col)}'
        for col in output_cols
    ]
    select_sql = ", ".join(select_parts)

    conn.execute(f"""
        COPY (
            SELECT {select_sql}
            FROM read_parquet('{in_sql}')
        ) TO '{escape_sql_string(input_path)}' (FORMAT PARQUET)
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
    parquet_files = [p for p in parquet_files if not p.name.endswith("_errors.parquet") and not p.name.startswith("tmp")]
    if not parquet_files:
        return 0
    for path in parquet_files:
        process_file(path, schema, log=log)
    return len(parquet_files)
