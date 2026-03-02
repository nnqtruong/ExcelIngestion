"""Step 04: Cast columns to schema types; flag failing rows to _errors.parquet sidecar."""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

# Project root (parent of scripts/)
ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
CLEAN_DIR = ROOT / "clean"
LOGS_DIR = ROOT / "logs"
SCHEMA_PATH = CONFIG_DIR / "schema.yaml"


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


def _cast_int64(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Cast to Int64; return (casted_series, error_mask)."""
    casted = pd.to_numeric(series, errors="coerce").astype("Int64")
    error = series.notna() & casted.isna()
    return casted, error


def _cast_float64(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Cast to float64; return (casted_series, error_mask)."""
    casted = pd.to_numeric(series, errors="coerce").astype("float64")
    error = series.notna() & casted.isna()
    return casted, error


def _cast_datetime64(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Cast to datetime64[ns]; return (casted_series, error_mask)."""
    casted = pd.to_datetime(series, errors="coerce")
    error = series.notna() & casted.isna()
    return casted, error


def _cast_bool(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Cast to boolean; return (casted_series, error_mask)."""
    true_vals = {True, "true", "True", "TRUE", "1", 1, "yes", "Yes"}
    false_vals = {False, "false", "False", "FALSE", "0", 0, "no", "No"}
    as_obj = series.astype("object")
    mask_true = as_obj.isin(true_vals)
    mask_false = as_obj.isin(false_vals)
    # None becomes pd.NA in BooleanDtype
    out = pd.Series(
        np.where(mask_true, True, np.where(mask_false, False, None)),
        index=series.index,
        dtype="boolean",
    )
    error = series.notna() & out.isna()
    return out, error


def _cast_string(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Cast to string; return (casted_series, error_mask)."""
    casted = series.astype("string")
    # Only flag as error if original was non-null but became <NA> (e.g. mixed types that couldn't convert)
    error = series.notna() & casted.isna()
    return casted, error


CASTERS = {
    "int64": _cast_int64,
    "float64": _cast_float64,
    "datetime64": _cast_datetime64,
    "bool": _cast_bool,
    "string": _cast_string,
}


def cast_column(series: pd.Series, schema_dtype: str) -> tuple[pd.Series, pd.Series]:
    """Cast series to schema dtype; return (casted_series, error_mask)."""
    normalized = (schema_dtype or "string").strip().lower()
    caster = CASTERS.get(normalized, _cast_string)
    return caster(series)


def _columns_as_list(schema: dict) -> list[dict]:
    """Return columns as list of {name, dtype, ...} (support list or dict schema)."""
    cols = schema["columns"]
    if isinstance(cols, list):
        return cols
    return [{"name": k, **v} for k, v in cols.items()]


def process_file(path: Path, schema: dict, log: logging.Logger) -> None:
    """
    Cast schema columns to expected types; put failing rows in path_stem_errors.parquet.
    Log error count per column.
    """
    df = pd.read_parquet(path)
    schema_cols = {c["name"]: c.get("dtype", "string") for c in _columns_as_list(schema)}

    # Columns we will cast (only those in both schema and df)
    to_cast = [c for c in df.columns if c in schema_cols]
    if not to_cast:
        log.info("%s: no schema columns to cast", path.name)
        return

    error_masks = {}
    casted = df.copy()

    for col in to_cast:
        dtype = schema_cols[col]
        casted[col], err = cast_column(casted[col], dtype)
        error_masks[col] = err

    # Rows that failed on at least one column
    row_has_error = pd.concat(error_masks.values(), axis=1).any(axis=1)

    # Log error count per column
    for col in to_cast:
        n = error_masks[col].sum()
        if n > 0:
            log.info("%s: column %s - %d cast error(s)", path.name, col, n)

    n_error_rows = row_has_error.sum()
    if n_error_rows == 0:
        # All rows passed; write casted df and remove sidecar if present
        casted.to_parquet(path, index=False)
        sidecar = path.parent / f"{path.stem}_errors.parquet"
        if sidecar.exists():
            sidecar.unlink()
            log.info("%s: removed empty sidecar %s", path.name, sidecar.name)
        return

    good = casted.loc[~row_has_error]
    bad = df.loc[row_has_error].copy()  # Keep original values for inspection

    good.to_parquet(path, index=False)
    sidecar = path.parent / f"{path.stem}_errors.parquet"
    bad.to_parquet(sidecar, index=False)
    log.info("%s: %d row(s) flagged to %s", path.name, n_error_rows, sidecar.name)


def main() -> None:
    setup_logging()
    log = logging.getLogger(__name__)

    schema = load_schema(SCHEMA_PATH)

    if not CLEAN_DIR.is_dir():
        log.error("No clean/ directory at %s", CLEAN_DIR)
        sys.exit(1)

    parquet_files = [
        p for p in sorted(CLEAN_DIR.glob("*.parquet"))
        if not p.name.endswith("_errors.parquet")
    ]
    if not parquet_files:
        log.info("No Parquet files in clean/")
        return

    for path in parquet_files:
        process_file(path, schema, log)
    log.info("Finished casting and flagging errors for %d file(s).", len(parquet_files))


if __name__ == "__main__":
    main()
