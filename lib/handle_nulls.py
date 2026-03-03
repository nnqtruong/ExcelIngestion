"""Step 07: Apply null-filling strategies from schema on combined output."""
import logging
from pathlib import Path

import pandas as pd

from lib.schema import columns_as_list, load_schema


def null_rate(series: pd.Series) -> float:
    """Fraction of nulls in series (0.0 to 1.0)."""
    n = len(series)
    return 0.0 if n == 0 else series.isna().sum() / n


def apply_fill_strategy(series: pd.Series, strategy: str) -> pd.Series:
    """Apply one fill strategy; return new series."""
    s = strategy.strip().lower()
    if s == "fill_zero":
        return series.fillna(0)
    if s == "fill_forward":
        return series.ffill()
    if s == "fill_backward":
        return series.bfill()
    if s == "fill_unknown":
        return series.fillna("Unknown")
    raise ValueError(f"Unknown fill_strategy: {strategy}")


def process_file(path: Path, schema: dict, log: logging.Logger) -> None:
    """Apply fill strategies per schema; log null rates before and after."""
    df = pd.read_parquet(path)
    n_rows = len(df)
    columns_with_strategy = [
        (c["name"], c["fill_strategy"])
        for c in columns_as_list(schema)
        if c.get("fill_strategy") and c["name"] in df.columns
    ]
    if not columns_with_strategy:
        log.info("%s: no columns with fill_strategy in schema", path.name)
        return

    n_cells = n_rows * len(df.columns) if n_rows else 0
    total_null_before = df.isna().sum().sum() / n_cells if n_cells else 0.0

    for col, strategy in columns_with_strategy:
        before = null_rate(df[col])
        df[col] = apply_fill_strategy(df[col], strategy)
        after = null_rate(df[col])
        log.info(
            "%s: column %s (strategy=%s) null rate before=%.2f%% after=%.2f%%",
            path.name, col, strategy, before * 100, after * 100,
        )

    total_null_after = df.isna().sum().sum() / n_cells if n_cells else 0.0
    log.info(
        "%s: overall null rate before=%.2f%% after=%.2f%%",
        path.name, total_null_before * 100, total_null_after * 100,
    )
    df.to_parquet(path, index=False)


def run_handle_nulls(combined_path: Path, schema_path: Path, log: logging.Logger) -> None:
    """Apply fill strategies to combined Parquet. combined_path must exist."""
    if not combined_path.exists():
        raise FileNotFoundError(f"Combined file not found: {combined_path}")
    schema = load_schema(schema_path)
    process_file(combined_path, schema, log)
