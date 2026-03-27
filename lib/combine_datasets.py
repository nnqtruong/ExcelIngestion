"""Step 06: Union cleaned Parquet files into one dataset with row_id (DuckDB, low memory)."""
import logging
from pathlib import Path

import duckdb
import psutil

from lib.config import get_combined_path, load_combine_config
from lib.logging_util import monitor_step


def get_parquet_files(clean_dir: Path) -> list[Path]:
    """Return sorted list of Parquet paths (exclude _errors and tmp_*)."""
    all_parquet = sorted(clean_dir.glob("*.parquet"))
    return [p for p in all_parquet if not p.name.endswith("_errors.parquet") and not p.name.startswith("tmp")]


def _escape_sql(s: str) -> str:
    """Escape single quotes for SQL string literal."""
    return s.replace("'", "''")


def combine_files(
    files: list[Path],
    output_path: Path,
    log: logging.Logger,
    primary_key: str | None = None,
) -> tuple[int, int]:
    """Union Parquet files via DuckDB (streaming); write to output_path. Returns (row_count, dupe_count)."""
    if not files:
        raise ValueError("No Parquet files to combine")

    output_path = Path(output_path).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    proc = psutil.Process()
    rss_before_mb = proc.memory_info().rss / 1024 / 1024
    log.info("Combine memory before: %.1f MB", rss_before_mb)

    conn = duckdb.connect()

    # Row counts per file (DuckDB reads Parquet without loading full file into RAM)
    total_from_files = 0
    for f in files:
        path_sql = _escape_sql(f.resolve().as_posix())
        n = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{path_sql}')").fetchone()[0]
        total_from_files += n
        log.info("Before combine: %s - %d rows", f.name, n)
    log.info("Before combine: total - %d rows", total_from_files)

    # Union all with source tracking; overwrite _source_file/_ingested_at if present
    # EXCLUDE avoids duplicate column names when parquet already has these columns
    union_parts = []
    for f in files:
        path_sql = _escape_sql(f.resolve().as_posix())
        name_sql = _escape_sql(f.name)
        # If parquet has _source_file/_ingested_at (from add_missing_columns), overwrite them
        union_parts.append(
            f"SELECT * EXCLUDE (_source_file, _ingested_at), "
            f"'{name_sql}' AS _source_file, CURRENT_TIMESTAMP AS _ingested_at "
            f"FROM read_parquet('{path_sql}')"
        )
    union_query = " UNION ALL ".join(union_parts)

    # Add row_id and write directly to Parquet (streaming; no full dataset in memory)
    out_sql = _escape_sql(str(output_path.as_posix()))
    conn.execute(f"""
        COPY (
            SELECT ROW_NUMBER() OVER () AS row_id, * FROM ({union_query}) sub
        ) TO '{out_sql}' (FORMAT PARQUET)
    """)

    row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{out_sql}')").fetchone()[0]
    dupe_count = 0
    if primary_key:
        dupe_count = conn.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT "{primary_key}")
            FROM read_parquet('{out_sql}')
        """).fetchone()[0]

    conn.close()

    rss_after_mb = proc.memory_info().rss / 1024 / 1024
    log.info("Combine memory after: %.1f MB", rss_after_mb)
    log.info("Added row_id primary key (1 to %d)", row_count)

    return row_count, dupe_count


@monitor_step
def run_combine_datasets(
    clean_dir: Path,
    analytics_dir: Path,
    combine_config_path: Path,
    log: logging.Logger,
    primary_key: str | None = None,
) -> Path:
    """Combine all Parquet in clean_dir via DuckDB, write to analytics/. Returns output path."""
    if not clean_dir.is_dir():
        raise FileNotFoundError(f"No clean/ directory at {clean_dir}")
    files = get_parquet_files(clean_dir)
    if not files:
        raise ValueError(f"No Parquet files to combine in {clean_dir}")

    output_path = get_combined_path(analytics_dir, combine_config_path)
    row_count, dupe_count = combine_files(files, output_path, log, primary_key=primary_key)

    log.info("After combine: %s - %d rows", output_path.name, row_count)
    if primary_key and dupe_count and dupe_count > 0:
        log.warning("Duplicate %s count: %d", primary_key, dupe_count)
    return output_path
