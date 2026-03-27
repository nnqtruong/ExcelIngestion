"""Step 04: Cast columns to schema types; flag failing rows to errors/ sidecar (DuckDB)."""
import logging
import os
import shutil
import tempfile
from pathlib import Path

import duckdb
import psutil

from lib.schema import columns_as_list, load_schema
from lib.logging_util import monitor_step

COERCE_DTYPES = {"int64", "float64", "datetime64"}
SCHEMA_DTYPE_TO_DUCKDB = {
    "int64": "BIGINT",
    "float64": "DOUBLE",
    "datetime64": "TIMESTAMP",
    "bool": "BOOLEAN",
    "string": "VARCHAR",
}


def _escape_sql(s: str) -> str:
    return s.replace("'", "''")


def _quote_id(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _cast_expr(col: str, schema_dtype: str) -> str:
    """Return DuckDB expression that casts column to schema type (returns NULL on failure)."""
    q = _quote_id(col)
    norm = (schema_dtype or "string").strip().lower()
    if norm == "int64":
        return f"TRY_CAST({q} AS BIGINT)"
    if norm == "float64":
        return f"TRY_CAST({q} AS DOUBLE)"
    if norm == "datetime64":
        return f"TRY_CAST({q} AS TIMESTAMP)"
    if norm == "bool":
        return (
            f"CASE WHEN LOWER(TRIM(CAST({q} AS VARCHAR))) IN ('true','1','yes') THEN true "
            f"WHEN LOWER(TRIM(CAST({q} AS VARCHAR))) IN ('false','0','no') THEN false ELSE NULL END"
        )
    return f"CAST({q} AS VARCHAR)"


def _good_cond(col: str, cast_expr: str) -> str:
    """SQL condition: column is null or cast succeeded."""
    q = _quote_id(col)
    return f"({q} IS NULL OR ({cast_expr}) IS NOT NULL)"


def _bad_cond(col: str, cast_expr: str) -> str:
    """SQL condition: column is not null and cast failed."""
    q = _quote_id(col)
    return f"({q} IS NOT NULL AND ({cast_expr}) IS NULL)"


def process_file(
    path: Path,
    schema: dict,
    errors_dir: Path,
    log: logging.Logger,
) -> None:
    """Cast schema columns via DuckDB TRY_CAST; write good rows to path, bad rows to errors/ (original values)."""
    proc = psutil.Process()
    rss_before_mb = proc.memory_info().rss / 1024 / 1024

    conn = duckdb.connect()
    input_path = path.resolve().as_posix()
    in_sql = _escape_sql(input_path)

    cur = conn.execute(f"SELECT * FROM read_parquet('{in_sql}') LIMIT 0")
    file_columns = [d[0] for d in cur.description]
    schema_cols = {c["name"]: c.get("dtype", "string") for c in columns_as_list(schema)}
    to_cast = [c for c in file_columns if c in schema_cols]

    if not to_cast:
        conn.close()
        log.info("%s: no schema columns to cast", path.name)
        return

    # Build SELECT for good output: cast each to_cast column, keep others
    select_parts = []
    cast_exprs = {}
    good_conds = []
    bad_conds = []
    for col in file_columns:
        if col in schema_cols:
            expr = _cast_expr(col, schema_cols[col])
            cast_exprs[col] = expr
            select_parts.append(f"{expr} AS {_quote_id(col)}")
            good_conds.append(_good_cond(col, expr))
            bad_conds.append(_bad_cond(col, expr))
        else:
            select_parts.append(_quote_id(col))

    select_sql = ", ".join(select_parts)
    good_where = " AND ".join(good_conds)
    bad_where = " OR ".join(bad_conds)

    # Count bad rows
    n_bad = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{in_sql}') WHERE {bad_where}"
    ).fetchone()[0]
    n_error_rows = n_bad

    # Log cast errors and first 5 bad values for coerce columns
    for col in to_cast:
        n = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{in_sql}') WHERE {_bad_cond(col, cast_exprs[col])}"
        ).fetchone()[0]
        if n > 0:
            log.info("%s: column %s - %d cast error(s)", path.name, col, n)
            if schema_cols.get(col, "").strip().lower() in COERCE_DTYPES:
                first_5 = conn.execute(
                    f"SELECT {_quote_id(col)} FROM read_parquet('{in_sql}') "
                    f"WHERE {_bad_cond(col, cast_exprs[col])} LIMIT 5"
                ).fetchall()
                vals = [row[0] for row in first_5]
                log.info("%s: column %s - first 5 coerced-to-NaN values: %s", path.name, col, vals)

    error_file = errors_dir / f"{path.stem}_errors.parquet"
    err_path_sql = _escape_sql((errors_dir / f"{path.stem}_errors.parquet").resolve().as_posix())

    # Write to temp file first, then replace original (Windows file locking workaround)
    tmp_fd, tmp_path_str = tempfile.mkstemp(suffix=".parquet", dir=path.parent)
    os.close(tmp_fd)
    tmp_path = Path(tmp_path_str)
    tmp_sql = _escape_sql(tmp_path.resolve().as_posix())

    try:
        if n_error_rows == 0:
            # All rows good: write casted output to temp, remove error file if present
            conn.execute(f"""
                COPY (SELECT {select_sql} FROM read_parquet('{in_sql}'))
                TO '{tmp_sql}' (FORMAT PARQUET)
            """)
            conn.close()
            shutil.move(str(tmp_path), str(path))
            if error_file.exists():
                error_file.unlink()
                log.info("%s: removed empty error file %s", path.name, error_file.name)
            rss_after_mb = proc.memory_info().rss / 1024 / 1024
            log.info("Clean errors %s: memory %.1f -> %.1f MB", path.name, rss_before_mb, rss_after_mb)
            return

        # Write bad rows (original values) to error sidecar first (while we still have full file)
        errors_dir.mkdir(parents=True, exist_ok=True)
        conn.execute(f"""
            COPY (SELECT * FROM read_parquet('{in_sql}') WHERE {bad_where})
            TO '{err_path_sql}' (FORMAT PARQUET)
        """)
        # Then write good rows (casted) to temp file
        conn.execute(f"""
            COPY (SELECT {select_sql} FROM read_parquet('{in_sql}') WHERE {good_where})
            TO '{tmp_sql}' (FORMAT PARQUET)
        """)
        conn.close()
        shutil.move(str(tmp_path), str(path))
    finally:
        # Clean up temp file if it still exists (e.g., on error)
        if tmp_path.exists():
            tmp_path.unlink()

    log.info("%s: %d row(s) flagged to errors/%s", path.name, n_error_rows, error_file.name)
    rss_after_mb = proc.memory_info().rss / 1024 / 1024
    log.info("Clean errors %s: memory %.1f -> %.1f MB", path.name, rss_before_mb, rss_after_mb)


@monitor_step
def run_clean_errors(
    clean_dir: Path,
    errors_dir: Path,
    schema_path: Path,
    log: logging.Logger,
) -> int:
    """Process all Parquet in clean_dir (exclude *_errors.parquet). Returns number of files processed."""
    schema = load_schema(schema_path)
    if not clean_dir.is_dir():
        raise FileNotFoundError(f"No clean/ directory at {clean_dir}")
    parquet_files = [
        p for p in sorted(clean_dir.glob("*.parquet"))
        if not p.name.endswith("_errors.parquet") and not p.name.startswith("tmp")
    ]
    if not parquet_files:
        return 0
    for path in parquet_files:
        process_file(path, schema, errors_dir, log)
    return len(parquet_files)
