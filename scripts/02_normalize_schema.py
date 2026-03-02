"""Step 02: Normalize column names and types to target schema."""

import re
import sys
from pathlib import Path

import pandas as pd
import yaml

# Project root (parent of scripts/)
ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
CLEAN_DIR = ROOT / "clean"
SCHEMA_PATH = CONFIG_DIR / "schema.yaml"


def to_snake_case(name: str) -> str:
    """Convert a string to lowercase snake_case."""
    s = re.sub(r"[^a-zA-Z0-9]+", "_", str(name).strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s.lower() if s else ""


def load_schema(path: Path) -> dict:
    """Load and return schema from schema.yaml."""
    if not path.exists():
        raise FileNotFoundError(f"Schema not found: {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not data or "columns" not in data:
        raise ValueError(f"Schema must define 'columns' list: {path}")
    return data


def _columns_as_list(schema: dict) -> list[dict]:
    """Return columns as list of {name, nullable?, ...} (support list or dict schema)."""
    cols = schema["columns"]
    if isinstance(cols, list):
        return cols
    return [{"name": k, **v} for k, v in cols.items()]


def get_required_columns(schema: dict) -> set[str]:
    """Return set of column names that are required (nullable: false)."""
    return {
        col["name"]
        for col in _columns_as_list(schema)
        if not col.get("nullable", True)
    }


def get_column_order(schema: dict) -> list[str]:
    """Return desired column order from schema."""
    return [col["name"] for col in _columns_as_list(schema)]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and snake_case all column names; return renamed DataFrame."""
    rename = {c: to_snake_case(c) for c in df.columns}
    return df.rename(columns=rename)


def reorder_columns(df: pd.DataFrame, order: list[str]) -> pd.DataFrame:
    """Reorder columns to match schema; extra columns appended at end."""
    ordered = [c for c in order if c in df.columns]
    extra = [c for c in df.columns if c not in order]
    return df[ordered + extra]


def process_file(path: Path, schema: dict) -> None:
    """Normalize one Parquet file: rename, reorder, overwrite. Missing columns added in step 03."""
    order = get_column_order(schema)

    df = pd.read_parquet(path)
    df = normalize_columns(df)

    # Note: Missing columns will be added by step 03 (03_add_missing_columns.py)
    # We don't fail here for missing columns - just normalize what exists

    df = reorder_columns(df, order)
    df.to_parquet(path, index=False)


def main() -> None:
    schema = load_schema(SCHEMA_PATH)

    if not CLEAN_DIR.is_dir():
        print(f"No clean/ directory at {CLEAN_DIR}", file=sys.stderr)
        sys.exit(1)

    parquet_files = list(CLEAN_DIR.glob("*.parquet"))
    if not parquet_files:
        print("No Parquet files in clean/")
        return

    for path in sorted(parquet_files):
        print(f"Normalizing {path.name} ...")
        process_file(path, schema)
    print(f"Done. Normalized {len(parquet_files)} file(s).")


if __name__ == "__main__":
    main()
