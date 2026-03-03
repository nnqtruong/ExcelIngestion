"""Step 04: Cast columns to schema types; flag failing rows to errors/ sidecar."""
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from lib.schema import columns_as_list, load_schema


def _cast_int64(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    casted = pd.to_numeric(series, errors="coerce").astype("Int64")
    error = series.notna() & casted.isna()
    return casted, error


def _cast_float64(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    casted = pd.to_numeric(series, errors="coerce").astype("float64")
    error = series.notna() & casted.isna()
    return casted, error


def _cast_datetime64(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    casted = pd.to_datetime(series, errors="coerce")
    error = series.notna() & casted.isna()
    return casted, error


def _cast_bool(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    true_vals = {True, "true", "True", "TRUE", "1", 1, "yes", "Yes"}
    false_vals = {False, "false", "False", "FALSE", "0", 0, "no", "No"}
    as_obj = series.astype("object")
    mask_true = as_obj.isin(true_vals)
    mask_false = as_obj.isin(false_vals)
    out = pd.Series(
        np.where(mask_true, True, np.where(mask_false, False, None)),
        index=series.index,
        dtype="boolean",
    )
    error = series.notna() & out.isna()
    return out, error


def _cast_string(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    casted = series.astype("string")
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
    normalized = (schema_dtype or "string").strip().lower()
    caster = CASTERS.get(normalized, _cast_string)
    return caster(series)


def process_file(
    path: Path,
    schema: dict,
    errors_dir: Path,
    log: logging.Logger,
) -> None:
    """Cast schema columns; put failing rows in errors/{stem}_errors.parquet."""
    df = pd.read_parquet(path)
    schema_cols = {c["name"]: c.get("dtype", "string") for c in columns_as_list(schema)}
    to_cast = [c for c in df.columns if c in schema_cols]
    if not to_cast:
        log.info("%s: no schema columns to cast", path.name)
        return

    error_masks = {}
    casted = df.copy()
    for col in to_cast:
        casted[col], err = cast_column(casted[col], schema_cols[col])
        error_masks[col] = err

    row_has_error = pd.concat(error_masks.values(), axis=1).any(axis=1)
    for col in to_cast:
        n = error_masks[col].sum()
        if n > 0:
            log.info("%s: column %s - %d cast error(s)", path.name, col, n)

    n_error_rows = row_has_error.sum()
    error_file = errors_dir / f"{path.stem}_errors.parquet"

    if n_error_rows == 0:
        casted.to_parquet(path, index=False)
        if error_file.exists():
            error_file.unlink()
            log.info("%s: removed empty error file %s", path.name, error_file.name)
        return

    good = casted.loc[~row_has_error]
    bad = df.loc[row_has_error].copy()
    good.to_parquet(path, index=False)
    errors_dir.mkdir(parents=True, exist_ok=True)
    bad.to_parquet(error_file, index=False)
    log.info("%s: %d row(s) flagged to errors/%s", path.name, n_error_rows, error_file.name)


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
        if not p.name.endswith("_errors.parquet")
    ]
    if not parquet_files:
        return 0
    for path in parquet_files:
        process_file(path, schema, errors_dir, log)
    return len(parquet_files)
