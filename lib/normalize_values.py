"""Step 05: Apply value_maps.yaml to standardize categorical values (DuckDB)."""
import logging
import os
import shutil
import tempfile
from pathlib import Path

import duckdb
import psutil

from lib.config import load_value_maps
from lib.sql_utils import escape_sql_string, quote_identifier
from lib.logging_util import monitor_step


def _case_expr(col: str, mapping: dict) -> str:
    """Build DuckDB CASE WHEN col = 'raw' THEN 'clean' ... ELSE col END."""
    q = quote_identifier(col)
    whens = []
    for raw, clean in mapping.items():
        raw_sql = "'" + escape_sql_string(str(raw)) + "'"
        clean_sql = "'" + escape_sql_string(str(clean)) + "'"
        whens.append(f"WHEN {q} = {raw_sql} THEN {clean_sql}")
    return "CASE " + " ".join(whens) + f" ELSE {q} END"


def process_file(path: Path, value_maps: dict, log: logging.Logger) -> None:
    """Apply value_maps via DuckDB CASE expressions; overwrite Parquet."""
    proc = psutil.Process()
    rss_before_mb = proc.memory_info().rss / 1024 / 1024

    conn = duckdb.connect()
    input_path = path.resolve().as_posix()
    in_sql = escape_sql_string(input_path)

    cur = conn.execute(f"SELECT * FROM read_parquet('{in_sql}') LIMIT 0")
    file_columns = [d[0] for d in cur.description]

    select_parts = []
    for col in file_columns:
        if col in value_maps and isinstance(value_maps[col], dict) and value_maps[col]:
            expr = _case_expr(col, value_maps[col])
            select_parts.append(f"{expr} AS {quote_identifier(col)}")
            # Count remapped (value changed and was not null)
            orig_sql = quote_identifier(col)
            n = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{in_sql}') "
                f"WHERE {orig_sql} IS NOT NULL AND ({orig_sql} != ({expr}))"
            ).fetchone()[0]
            if n > 0:
                log.info("%s: column %s - %d value(s) remapped", path.name, col, n)
        else:
            select_parts.append(quote_identifier(col))
        if col in value_maps and not isinstance(value_maps[col], dict):
            log.warning("value_maps[%s] is not a dict, skipping", col)

    select_sql = ", ".join(select_parts)

    # Write to temp file first, then replace original (Windows file locking workaround)
    tmp_fd, tmp_path_str = tempfile.mkstemp(suffix=".parquet", dir=path.parent)
    os.close(tmp_fd)
    tmp_path = Path(tmp_path_str)
    tmp_sql = escape_sql_string(tmp_path.resolve().as_posix())

    try:
        conn.execute(f"""
            COPY (SELECT {select_sql} FROM read_parquet('{in_sql}'))
            TO '{tmp_sql}' (FORMAT PARQUET)
        """)
        conn.close()
        shutil.move(str(tmp_path), str(path))
    finally:
        # Clean up temp file if it still exists (e.g., on error)
        if tmp_path.exists():
            tmp_path.unlink()

    rss_after_mb = proc.memory_info().rss / 1024 / 1024
    log.info("Normalize values %s: memory %.1f -> %.1f MB", path.name, rss_before_mb, rss_after_mb)


@monitor_step
def run_normalize_values(
    clean_dir: Path,
    value_maps_path: Path,
    log: logging.Logger,
) -> int:
    """Process all Parquet in clean_dir via DuckDB (exclude *_errors). Returns number of files processed."""
    value_maps = load_value_maps(value_maps_path)
    if not value_maps:
        return 0
    if not clean_dir.is_dir():
        raise FileNotFoundError(f"No clean/ directory at {clean_dir}")
    parquet_files = [
        p for p in sorted(clean_dir.glob("*.parquet"))
        if not p.name.endswith("_errors.parquet") and not p.name.startswith("tmp")
    ]
    if not parquet_files:
        return 0
    for path in parquet_files:
        process_file(path, value_maps, log)
    return len(parquet_files)
