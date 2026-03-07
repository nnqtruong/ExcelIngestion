"""Step 07: Apply null-filling strategies from schema on combined output (DuckDB)."""
import logging
from pathlib import Path

import duckdb
import psutil

from lib.schema import columns_as_list, load_schema
from lib.logging_util import monitor_step


def _escape_sql(s: str) -> str:
    return s.replace("'", "''")


def _quote_id(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _fill_expr(col: str, strategy: str, has_row_id: bool) -> str:
    """Return DuckDB expression for fill_strategy (e.g. COALESCE(col, 0))."""
    q = _quote_id(col)
    s = (strategy or "").strip().lower()
    if s == "fill_zero":
        return f"COALESCE({q}, 0)"
    if s == "fill_unknown":
        return f"COALESCE({q}, 'Unknown')"
    if s == "fill_forward" and has_row_id:
        return f"COALESCE({q}, LAST_VALUE({q} IGNORE NULLS) OVER (ORDER BY row_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))"
    if s == "fill_backward" and has_row_id:
        return f"COALESCE({q}, FIRST_VALUE({q} IGNORE NULLS) OVER (ORDER BY row_id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING))"
    return q


def process_file(path: Path, schema: dict, log: logging.Logger) -> None:
    """Apply fill strategies via DuckDB; overwrite combined Parquet."""
    proc = psutil.Process()
    rss_before_mb = proc.memory_info().rss / 1024 / 1024

    conn = duckdb.connect()
    input_path = path.resolve().as_posix()
    in_sql = _escape_sql(input_path)

    cur = conn.execute(f"SELECT * FROM read_parquet('{in_sql}') LIMIT 0")
    file_columns = [d[0] for d in cur.description]
    has_row_id = "row_id" in file_columns

    schema_cols = {c["name"]: c for c in columns_as_list(schema)}
    columns_with_strategy = [
        (c["name"], c.get("fill_strategy"))
        for c in columns_as_list(schema)
        if c.get("fill_strategy") and c["name"] in file_columns
    ]
    # Skip null/None/empty strategy
    columns_with_strategy = [(n, s) for n, s in columns_with_strategy if s and str(s).strip().lower() not in ("null", "")]

    if not columns_with_strategy:
        conn.close()
        log.info("%s: no columns with fill_strategy in schema", path.name)
        rss_after_mb = proc.memory_info().rss / 1024 / 1024
        log.info("Handle nulls %s: memory %.1f -> %.1f MB", path.name, rss_before_mb, rss_after_mb)
        return

    n_rows = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{in_sql}')").fetchone()[0]
    strategy_by_col = {n: s for n, s in columns_with_strategy}

    select_parts = []
    for col in file_columns:
        strategy = strategy_by_col.get(col)
        if strategy:
            expr = _fill_expr(col, strategy, has_row_id)
            select_parts.append(f"{expr} AS {_quote_id(col)}")
            before_rate = conn.execute(
                f"SELECT COUNT(*) FILTER (WHERE {_quote_id(col)} IS NULL) * 100.0 / NULLIF(COUNT(*), 0) FROM read_parquet('{in_sql}')"
            ).fetchone()[0] or 0.0
            log.info(
                "%s: column %s (strategy=%s) null rate before=%.2f%%",
                path.name, col, strategy, before_rate,
            )
        else:
            select_parts.append(_quote_id(col))

    select_sql = ", ".join(select_parts)
    conn.execute(f"""
        COPY (SELECT {select_sql} FROM read_parquet('{in_sql}'))
        TO '{in_sql}' (FORMAT PARQUET)
    """)
    conn.close()

    rss_after_mb = proc.memory_info().rss / 1024 / 1024
    log.info("Handle nulls %s: memory %.1f -> %.1f MB", path.name, rss_before_mb, rss_after_mb)


@monitor_step
def run_handle_nulls(combined_path: Path, schema_path: Path, log: logging.Logger) -> None:
    """Apply fill strategies to combined Parquet via DuckDB. combined_path must exist."""
    if not combined_path.exists():
        raise FileNotFoundError(f"Combined file not found: {combined_path}")
    schema = load_schema(schema_path)
    process_file(combined_path, schema, log)
