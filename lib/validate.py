"""Step 08: Validate row count, required columns, duplicates, null rates, dtypes via DuckDB."""
import json
from pathlib import Path

import duckdb

from lib.config import load_combine_config
from lib.schema import load_schema
from lib.sql_utils import escape_sql_string, quote_identifier
from lib.logging_util import monitor_step


def _json_serial(obj):
    """Convert types to native Python for JSON."""
    if hasattr(obj, "item"):  # numpy scalar
        return obj.item()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def get_columns_list(schema: dict) -> list[tuple[str, dict]]:
    """Return [(column_name, spec), ...]. Supports list or dict columns."""
    cols = schema["columns"]
    if isinstance(cols, list):
        return [(c["name"], c) for c in cols]
    return list(cols.items())


def get_required_columns(schema: dict) -> list[str]:
    """Columns that must be present (nullable: false or primary_key: true)."""
    out = []
    for name, spec in get_columns_list(schema):
        s = spec if isinstance(spec, dict) else {}
        if not s.get("nullable", True) or s.get("primary_key"):
            out.append(name)
    return out


def get_primary_key(schema: dict, combine_config: dict | None) -> list[str]:
    """Primary key: combine_config primary_key, or schema primary_key column(s)."""
    if combine_config and combine_config.get("primary_key"):
        pk = combine_config["primary_key"]
        return [pk] if isinstance(pk, str) else list(pk)
    out = []
    for name, spec in get_columns_list(schema):
        s = spec if isinstance(spec, dict) else {}
        if s.get("primary_key"):
            out.append(name)
    return out


def get_expected_dtype(spec: dict) -> str:
    d = (spec.get("dtype") or "string")
    return str(d).strip().lower()


def duckdb_dtype_to_normalized(duckdb_type: str) -> str:
    """Map DuckDB type string to normalized schema dtype (int64, float64, datetime64, bool, string)."""
    d = (duckdb_type or "").upper()
    if "INT" in d or d == "BIGINT":
        return "int64"
    if "DOUBLE" in d or "FLOAT" in d:
        return "float64"
    if "TIMESTAMP" in d or "DATE" in d:
        return "datetime64"
    if "BOOL" in d:
        return "bool"
    return "string"


def dtype_matches(schema_dtype: str, actual_dtype: str) -> bool:
    return get_expected_dtype({"dtype": schema_dtype}) == actual_dtype


def run_validation(
    path: Path,
    schema: dict,
    combine_config: dict | None,
) -> dict:
    """Run all checks via DuckDB queries against Parquet. Return report dict with 'passed' and 'checks'."""
    conn = duckdb.connect()
    input_path = path.resolve().as_posix()
    in_sql = escape_sql_string(input_path)
    tbl = f"read_parquet('{in_sql}')"

    n_rows = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    cols_list = get_columns_list(schema)
    validation = schema.get("validation") or {}
    max_null_rate = float(validation.get("max_null_rate", 1.0))
    max_duplicate_rate = float(validation.get("max_duplicate_rate", 1.0))
    min_row_count = int(validation.get("min_row_count", 0))

    report = {"file": str(path), "row_count": n_rows, "checks": {}, "passed": True}

    # Row count
    row_count_ok = n_rows >= min_row_count
    report["checks"]["row_count"] = {
        "min_row_count": min_row_count,
        "actual": n_rows,
        "passed": row_count_ok,
    }
    if not row_count_ok:
        report["passed"] = False

    # Required columns: get file columns from DuckDB
    cur = conn.execute(f"SELECT * FROM {tbl} LIMIT 0")
    file_columns = [d[0] for d in cur.description]
    required = get_required_columns(schema)
    missing = [c for c in required if c not in file_columns]
    required_ok = len(missing) == 0
    report["checks"]["required_columns"] = {
        "required": required,
        "missing": missing,
        "passed": required_ok,
    }
    if not required_ok:
        report["passed"] = False

    # Duplicate rate (primary key)
    primary_key = get_primary_key(schema, combine_config)
    if primary_key:
        pk_missing = [c for c in primary_key if c not in file_columns]
        if pk_missing:
            report["checks"]["duplicate_rate"] = {
                "primary_key": primary_key,
                "error": f"primary_key columns missing: {pk_missing}",
                "passed": False,
            }
            report["passed"] = False
        else:
            if len(primary_key) == 1:
                pk_col = quote_identifier(primary_key[0])
                n_dup = conn.execute(
                    f"SELECT SUM(c) FROM (SELECT {pk_col}, COUNT(*) AS c FROM {tbl} GROUP BY {pk_col} HAVING COUNT(*) > 1) sub"
                ).fetchone()[0] or 0
            else:
                pk_list = ", ".join(quote_identifier(c) for c in primary_key)
                n_dup = conn.execute(
                    f"SELECT SUM(c) FROM (SELECT {pk_list}, COUNT(*) AS c FROM {tbl} GROUP BY {pk_list} HAVING COUNT(*) > 1) sub"
                ).fetchone()[0] or 0
            duplicate_rate = n_dup / n_rows if n_rows else 0.0
            dup_ok = duplicate_rate <= max_duplicate_rate
            report["checks"]["duplicate_rate"] = {
                "primary_key": primary_key,
                "duplicate_rows": int(n_dup),
                "duplicate_rate": round(duplicate_rate, 6),
                "max_duplicate_rate": max_duplicate_rate,
                "passed": dup_ok,
            }
            if not dup_ok:
                report["passed"] = False
    else:
        report["checks"]["duplicate_rate"] = {
            "primary_key": None,
            "skipped": "no primary_key in schema or combine.yaml",
            "passed": True,
        }

    # Null rate per column (DuckDB: COUNT(*) FILTER (WHERE col IS NULL) * 100.0 / COUNT(*))
    null_rates = {}
    null_ok = True
    for name, spec in cols_list:
        if name not in file_columns:
            continue
        q = quote_identifier(name)
        rate = conn.execute(
            f"SELECT COUNT(*) FILTER (WHERE {q} IS NULL) * 1.0 / NULLIF(COUNT(*), 0) FROM {tbl}"
        ).fetchone()[0]
        rate = float(rate) if rate is not None else 0.0
        null_rates[name] = round(rate, 6)
        col_max = (spec if isinstance(spec, dict) else {}).get("max_null_rate")
        threshold = float(col_max) if col_max is not None else max_null_rate
        if rate > threshold:
            null_ok = False
    report["checks"]["null_rate_per_column"] = {
        "max_null_rate": max_null_rate,
        "per_column": null_rates,
        "passed": null_ok,
    }
    if not null_ok:
        report["passed"] = False

    # Dtype: get from DuckDB DESCRIBE
    describe_rows = conn.execute(f"DESCRIBE SELECT * FROM {tbl}").fetchall()
    # DESCRIBE returns (column_name, column_type, null, default, key); use first two
    file_dtypes = {row[0]: (row[1] if len(row) > 1 else "VARCHAR") for row in describe_rows}
    dtype_results = {}
    dtype_ok = True
    for name, spec in cols_list:
        if name not in file_columns:
            continue
        expected = get_expected_dtype(spec if isinstance(spec, dict) else {})
        actual_raw = file_dtypes.get(name, "VARCHAR")
        actual = duckdb_dtype_to_normalized(actual_raw)
        ok = dtype_matches(expected, actual)
        dtype_results[name] = {"expected": expected, "actual": actual, "passed": ok}
        if not ok:
            dtype_ok = False
    report["checks"]["dtype_correctness"] = {"per_column": dtype_results, "passed": dtype_ok}
    if not dtype_ok:
        report["passed"] = False

    conn.close()

    return report


@monitor_step
def run_validate(
    combined_path: Path,
    schema_path: Path,
    combine_config_path: Path,
    report_path: Path,
) -> dict:
    """Load schema and combine config, run validation via DuckDB, write report. Returns report dict."""
    schema = load_schema(schema_path)
    combine_config = load_combine_config(combine_config_path)
    if not combine_config:
        combine_config = None
    if not combined_path.exists():
        raise FileNotFoundError(f"Combined file not found: {combined_path}")
    report = run_validation(combined_path, schema, combine_config)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=_json_serial)
    return report
