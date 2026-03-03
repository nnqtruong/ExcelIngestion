"""Step 06: Union cleaned Parquet files into one dataset with row_id."""
import logging
from pathlib import Path

import pandas as pd

from lib.config import get_combined_path, load_combine_config


def get_parquet_files(clean_dir: Path) -> list[Path]:
    """Return sorted list of Parquet paths (exclude _errors)."""
    all_parquet = sorted(clean_dir.glob("*.parquet"))
    return [p for p in all_parquet if not p.name.endswith("_errors.parquet")]


def combine_files(files: list[Path], log: logging.Logger) -> pd.DataFrame:
    """Read and concatenate all Parquet files. Log row counts."""
    if not files:
        raise ValueError("No Parquet files to combine")
    dfs = []
    total_rows = 0
    for path in files:
        df = pd.read_parquet(path)
        n = len(df)
        total_rows += n
        log.info("Before combine: %s - %d rows", path.name, n)
        dfs.append(df)
    log.info("Before combine: total - %d rows", total_rows)
    return pd.concat(dfs, axis=0, ignore_index=True)


def add_row_id(df: pd.DataFrame, log: logging.Logger) -> pd.DataFrame:
    """Add row_id column as first column (1-based integer primary key)."""
    df = df.copy()
    df.insert(0, "row_id", range(1, len(df) + 1))
    log.info("Added row_id primary key (1 to %d)", len(df))
    return df


def run_combine_datasets(
    clean_dir: Path,
    analytics_dir: Path,
    combine_config_path: Path,
    log: logging.Logger,
) -> Path:
    """Combine all Parquet in clean_dir, add row_id, write to analytics/. Returns output path."""
    if not clean_dir.is_dir():
        raise FileNotFoundError(f"No clean/ directory at {clean_dir}")
    files = get_parquet_files(clean_dir)
    if not files:
        raise ValueError(f"No Parquet files to combine in {clean_dir}")
    combined = combine_files(files, log)
    combined = add_row_id(combined, log)
    output_path = get_combined_path(analytics_dir, combine_config_path)
    analytics_dir.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output_path, index=False)
    log.info("After combine: %s - %d rows", output_path.name, len(combined))
    return output_path
