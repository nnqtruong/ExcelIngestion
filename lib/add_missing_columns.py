"""Step 03: Add missing schema columns with nulls and correct dtype."""
import logging
from pathlib import Path

import pandas as pd

from lib.schema import columns_as_list, load_schema

SCHEMA_DTYPE_TO_PANDAS = {
    "string": "string",
    "int64": "Int64",
    "float64": "float64",
    "datetime64": "datetime64[ns]",
    "bool": "boolean",
}


def get_pandas_dtype(schema_dtype: str) -> str:
    """Map schema dtype to pandas dtype (nullable where applicable)."""
    normalized = (schema_dtype or "string").strip().lower()
    return SCHEMA_DTYPE_TO_PANDAS.get(normalized, "string")


def add_missing_columns(df: pd.DataFrame, schema: dict) -> list[str]:
    """Add any schema columns missing from df with null and correct dtype. Returns list of added names."""
    added = []
    for col_spec in columns_as_list(schema):
        name = col_spec["name"]
        if name in df.columns:
            continue
        dtype = get_pandas_dtype(col_spec.get("dtype", "string"))
        df[name] = pd.Series(dtype=dtype)
        added.append(name)
    return added


def process_file(path: Path, schema: dict, log: logging.Logger) -> None:
    """Add missing columns to one Parquet file; log additions and overwrite."""
    df = pd.read_parquet(path)
    added = add_missing_columns(df, schema)
    if added:
        order = [c["name"] for c in columns_as_list(schema)]
        extra = [c for c in df.columns if c not in order]
        df = df[order + extra]
        df.to_parquet(path, index=False)
    for col in added:
        log.info("Added column %s to %s", col, path.name)


def run_add_missing_columns(clean_dir: Path, schema_path: Path, log: logging.Logger) -> int:
    """Process all Parquet in clean_dir. Returns number of files processed."""
    schema = load_schema(schema_path)
    if not clean_dir.is_dir():
        raise FileNotFoundError(f"No clean/ directory at {clean_dir}")
    parquet_files = list(clean_dir.glob("*.parquet"))
    if not parquet_files:
        return 0
    for path in sorted(parquet_files):
        process_file(path, schema, log)
    return len(parquet_files)
