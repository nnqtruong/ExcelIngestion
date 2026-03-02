"""Step 08: Final gate. Validate row count, required columns, duplicates, null rates, dtypes; output report; fail if thresholds breached."""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


def _json_serial(obj):
    """Convert numpy/pandas types to native Python for JSON."""
    if isinstance(obj, (np.bool_, np.integer)):
        return bool(obj) if isinstance(obj, np.bool_) else int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# Project root (parent of scripts/)
ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
ANALYTICS_DIR = ROOT / "analytics"
LOGS_DIR = ROOT / "logs"
SCHEMA_PATH = CONFIG_DIR / "schema.yaml"
COMBINE_PATH = CONFIG_DIR / "combine.yaml"
REPORT_PATH = LOGS_DIR / "validation_report.json"


def load_schema(path: Path) -> dict:
    """Load schema.yaml."""
    if not path.exists():
        raise FileNotFoundError(f"Schema not found: {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not data or "columns" not in data:
        raise ValueError(f"Schema must define 'columns': {path}")
    return data


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
    """Primary key for duplicate check: combine.yaml primary_key, or schema primary_key column(s)."""
    if combine_config and combine_config.get("primary_key"):
        pk = combine_config["primary_key"]
        return [pk] if isinstance(pk, str) else list(pk)
    out = []
    for name, spec in get_columns_list(schema):
        s = spec if isinstance(spec, dict) else {}
        if s.get("primary_key"):
            out.append(name)
    return out


def get_combined_path() -> Path:
    """Path to combined Parquet (from combine.yaml or default)."""
    if COMBINE_PATH.exists():
        with open(COMBINE_PATH, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        if config and config.get("output"):
            return ANALYTICS_DIR / config["output"]
    return ANALYTICS_DIR / "combined.parquet"


def get_expected_dtype(spec: dict) -> str:
    """Normalize schema dtype to a string for comparison."""
    d = (spec.get("dtype") or "string")
    return str(d).strip().lower()


def pandas_dtype_normalized(series: pd.Series) -> str:
    """Normalize pandas dtype to schema-like string for comparison."""
    d = str(series.dtype).lower()
    if d in ("int64", "int32") or d.startswith("int"):
        return "int64"
    if d in ("float64", "float32") or d.startswith("float"):
        return "float64"
    if "datetime" in d:
        return "datetime64"
    if d in ("bool", "boolean"):
        return "bool"
    return "string"


def dtype_matches(schema_dtype: str, actual_dtype: str) -> bool:
    """Compare expected vs actual dtype (normalized)."""
    exp = get_expected_dtype({"dtype": schema_dtype})
    return exp == actual_dtype


def run_validation(path: Path, schema: dict, combine_config: dict | None) -> dict:
    """Run all checks; return report dict (and set 'passed' per check)."""
    df = pd.read_parquet(path)
    n_rows = len(df)
    cols_list = get_columns_list(schema)
    validation = schema.get("validation") or {}
    max_null_rate = float(validation.get("max_null_rate", 1.0))
    max_duplicate_rate = float(validation.get("max_duplicate_rate", 1.0))
    min_row_count = int(validation.get("min_row_count", 0))

    report = {
        "file": str(path),
        "row_count": n_rows,
        "checks": {},
        "passed": True,
    }

    # 1. Row count consistency
    row_count_ok = n_rows >= min_row_count
    report["checks"]["row_count"] = {
        "min_row_count": min_row_count,
        "actual": n_rows,
        "passed": row_count_ok,
    }
    if not row_count_ok:
        report["passed"] = False

    # 2. Required columns present
    required = get_required_columns(schema)
    missing = [c for c in required if c not in df.columns]
    required_ok = len(missing) == 0
    report["checks"]["required_columns"] = {
        "required": required,
        "missing": missing,
        "passed": required_ok,
    }
    if not required_ok:
        report["passed"] = False

    # 3. Duplicate rate (on primary key)
    primary_key = get_primary_key(schema, combine_config)
    if primary_key:
        pk_missing = [c for c in primary_key if c not in df.columns]
        if pk_missing:
            report["checks"]["duplicate_rate"] = {
                "primary_key": primary_key,
                "error": f"primary_key columns missing: {pk_missing}",
                "passed": False,
            }
            report["passed"] = False
        else:
            dup_mask = df.duplicated(subset=primary_key, keep=False)
            n_dup = dup_mask.sum()
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

    # 4. Null rate per column (schema columns only)
    null_rates = {}
    null_ok = True
    for name, spec in cols_list:
        if name not in df.columns:
            continue
        rate = float(df[name].isna().sum() / n_rows) if n_rows else 0.0
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

    # 5. Dtype correctness
    dtype_results = {}
    dtype_ok = True
    for name, spec in cols_list:
        if name not in df.columns:
            continue
        expected = get_expected_dtype(spec if isinstance(spec, dict) else {})
        actual = pandas_dtype_normalized(df[name])
        ok = dtype_matches(expected, actual)
        dtype_results[name] = {"expected": expected, "actual": actual, "passed": ok}
        if not ok:
            dtype_ok = False
    report["checks"]["dtype_correctness"] = {
        "per_column": dtype_results,
        "passed": dtype_ok,
    }
    if not dtype_ok:
        report["passed"] = False

    return report


def main() -> None:
    schema = load_schema(SCHEMA_PATH)
    combine_config = None
    if COMBINE_PATH.exists():
        with open(COMBINE_PATH, encoding="utf-8") as f:
            combine_config = yaml.safe_load(f)

    path = get_combined_path()
    if not path.exists():
        print(f"Validation failed: combined file not found: {path}", file=sys.stderr)
        sys.exit(1)

    report = run_validation(path, schema, combine_config)

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=_json_serial)

    print(f"Validation report written to {REPORT_PATH}")
    if report["passed"]:
        print("All checks passed.")
    else:
        print("Validation failed: one or more checks breached thresholds.", file=sys.stderr)
        for check_name, check in report["checks"].items():
            if not check.get("passed", True):
                print(f"  FAIL: {check_name} - {check}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
