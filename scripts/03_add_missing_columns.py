"""Step 03: Add missing columns with null values and correct dtype per schema."""

import logging
import sys
from pathlib import Path

import pandas as pd
import yaml

# Project root (parent of scripts/)
ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
CLEAN_DIR = ROOT / "clean"
LOGS_DIR = ROOT / "logs"
SCHEMA_PATH = CONFIG_DIR / "schema.yaml"

# Pandas dtypes that support nulls for each schema dtype
SCHEMA_DTYPE_TO_PANDAS = {
    "string": "string",
    "int64": "Int64",
    "float64": "float64",
    "datetime64": "datetime64[ns]",
    "bool": "boolean",
}


def setup_logging() -> None:
    """Configure logging to pipeline.log and console."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / "pipeline.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def load_schema(path: Path) -> dict:
    """Load and return schema from schema.yaml."""
    if not path.exists():
        raise FileNotFoundError(f"Schema not found: {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not data or "columns" not in data:
        raise ValueError(f"Schema must define 'columns' list: {path}")
    return data


def get_pandas_dtype(schema_dtype: str) -> str:
    """Map schema dtype to pandas dtype (nullable where applicable)."""
    normalized = (schema_dtype or "string").strip().lower()
    return SCHEMA_DTYPE_TO_PANDAS.get(normalized, "string")


def _columns_as_list(schema: dict) -> list[dict]:
    """Return columns as list of {name, dtype, ...} (support list or dict schema)."""
    cols = schema["columns"]
    if isinstance(cols, list):
        return cols
    return [{"name": k, **v} for k, v in cols.items()]


def add_missing_columns(df: pd.DataFrame, schema: dict) -> list[str]:
    """
    Add any schema columns missing from df with null values and correct dtype.
    Returns list of column names that were added.
    """
    added = []
    for col_spec in _columns_as_list(schema):
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
        for col in added:
            log.info("Added column %s to %s", col, path.name)
        # Reorder to match schema so new columns sit in the right place
        order = [c["name"] for c in _columns_as_list(schema)]
        extra = [c for c in df.columns if c not in order]
        df = df[order + extra]
        df.to_parquet(path, index=False)


def main() -> None:
    setup_logging()
    log = logging.getLogger(__name__)

    schema = load_schema(SCHEMA_PATH)

    if not CLEAN_DIR.is_dir():
        log.error("No clean/ directory at %s", CLEAN_DIR)
        sys.exit(1)

    parquet_files = list(CLEAN_DIR.glob("*.parquet"))
    if not parquet_files:
        log.info("No Parquet files in clean/")
        return

    for path in sorted(parquet_files):
        process_file(path, schema, log)
    log.info("Finished adding missing columns for %d file(s).", len(parquet_files))


if __name__ == "__main__":
    main()
