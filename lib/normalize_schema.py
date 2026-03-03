"""Step 02: Normalize column names to snake_case and reorder to schema."""
import re
from pathlib import Path

import pandas as pd

from lib.schema import get_column_order, load_schema


def to_snake_case(name: str) -> str:
    """Convert a string to lowercase snake_case."""
    s = re.sub(r"[^a-zA-Z0-9]+", "_", str(name).strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s.lower() if s else ""


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and snake_case all column names."""
    rename = {c: to_snake_case(c) for c in df.columns}
    return df.rename(columns=rename)


def reorder_columns(df: pd.DataFrame, order: list[str]) -> pd.DataFrame:
    """Reorder columns to match schema; extra columns appended at end."""
    ordered = [c for c in order if c in df.columns]
    extra = [c for c in df.columns if c not in order]
    return df[ordered + extra]


def process_file(path: Path, schema: dict) -> None:
    """Normalize one Parquet file: rename, reorder, overwrite."""
    order = get_column_order(schema)
    df = pd.read_parquet(path)
    df = normalize_columns(df)
    df = reorder_columns(df, order)
    df.to_parquet(path, index=False)


def run_normalize_schema(clean_dir: Path, schema_path: Path) -> int:
    """Normalize all Parquet in clean_dir per schema. Returns number of files processed."""
    schema = load_schema(schema_path)
    if not clean_dir.is_dir():
        raise FileNotFoundError(f"No clean/ directory at {clean_dir}")
    parquet_files = sorted(clean_dir.glob("*.parquet"))
    if not parquet_files:
        return 0
    for path in parquet_files:
        process_file(path, schema)
    return len(parquet_files)
