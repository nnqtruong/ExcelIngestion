"""Step 05: Apply value_maps.yaml to standardize categorical values (DuckDB)."""
import logging
from pathlib import Path

import duckdb
import psutil
import yaml

from lib.logging_util import monitor_step


def load_value_maps(path: Path) -> dict:
    """Load value_maps.yaml: { column_name: { source_value: target_value } }."""
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"value_maps.yaml must be a dict (column -> map): {path}")
    return data


def _escape_sql(s: str) -> str:
    return str(s).replace("'", "''")


def _quote_id(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _case_expr(col: str, mapping: dict) -> str:
    """Build DuckDB CASE WHEN col = 'raw' THEN 'clean' ... ELSE col END."""
    q = _quote_id(col)
    whens = []
    for raw, clean in mapping.items():
        raw_sql = "'" + _escape_sql(raw) + "'"
        clean_sql = "'" + _escape_sql(clean) + "'"
        whens.append(f"WHEN {q} = {raw_sql} THEN {clean_sql}")
    return "CASE " + " ".join(whens) + f" ELSE {q} END"


def process_file(path: Path, value_maps: dict, log: logging.Logger) -> None:
    """Apply value_maps via DuckDB CASE expressions; overwrite Parquet."""
    proc = psutil.Process()
    rss_before_mb = proc.memory_info().rss / 1024 / 1024

    conn = duckdb.connect()
    input_path = path.resolve().as_posix()
    in_sql = _escape_sql(input_path)

    cur = conn.execute(f"SELECT * FROM read_parquet('{in_sql}') LIMIT 0")
    file_columns = [d[0] for d in cur.description]

    select_parts = []
    for col in file_columns:
        if col in value_maps and isinstance(value_maps[col], dict) and value_maps[col]:
            expr = _case_expr(col, value_maps[col])
            select_parts.append(f"{expr} AS {_quote_id(col)}")
            # Count remapped (value changed and was not null)
            orig_sql = _quote_id(col)
            n = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{in_sql}') "
                f"WHERE {orig_sql} IS NOT NULL AND ({orig_sql} != ({expr}))"
            ).fetchone()[0]
            if n > 0:
                log.info("%s: column %s - %d value(s) remapped", path.name, col, n)
        else:
            select_parts.append(_quote_id(col))
        if col in value_maps and not isinstance(value_maps[col], dict):
            log.warning("value_maps[%s] is not a dict, skipping", col)

    select_sql = ", ".join(select_parts)
    conn.execute(f"""
        COPY (SELECT {select_sql} FROM read_parquet('{in_sql}'))
        TO '{in_sql}' (FORMAT PARQUET)
    """)
    conn.close()

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
        if not p.name.endswith("_errors.parquet")
    ]
    if not parquet_files:
        return 0
    for path in parquet_files:
        process_file(path, value_maps, log)
    return len(parquet_files)
