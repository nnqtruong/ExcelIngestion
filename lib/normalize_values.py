"""Step 05: Apply value_maps.yaml to standardize categorical values."""
import logging
from pathlib import Path

import pandas as pd
import yaml


def load_value_maps(path: Path) -> dict:
    """Load value_maps.yaml: { column_name: { source_value: target_value } }."""
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"value_maps.yaml must be a dict (column -> map): {path}")
    return data


def apply_column_map(series: pd.Series, mapping: dict) -> tuple[pd.Series, int]:
    """Replace values using mapping; return (new_series, count_remapped)."""
    if not mapping:
        return series, 0
    before = series
    after = before.replace(mapping)
    n = ((before != after) & before.notna()).sum()
    return after, int(n)


def process_file(path: Path, value_maps: dict, log: logging.Logger) -> None:
    """Apply value_maps to matching columns; log remap count per column."""
    df = pd.read_parquet(path)
    for col, mapping in value_maps.items():
        if col not in df.columns:
            continue
        if not isinstance(mapping, dict):
            log.warning("value_maps[%s] is not a dict, skipping", col)
            continue
        df[col], n = apply_column_map(df[col], mapping)
        if n > 0:
            log.info("%s: column %s - %d value(s) remapped", path.name, col, n)
    df.to_parquet(path, index=False)


def run_normalize_values(
    clean_dir: Path,
    value_maps_path: Path,
    log: logging.Logger,
) -> int:
    """Process all Parquet in clean_dir (exclude *_errors). Returns number of files processed."""
    value_maps = load_value_maps(value_maps_path)
    if not value_maps:
        return 0
    if not clean_dir.is_dir():
        raise FileNotFoundError(f"No clean/ directory at {clean_dir}")
    parquet_files = [
        p for p in sorted(clean_dir.glob("*.parquet"))
        if not p.name.endswith("_errors.parquet")
    ]
    if not parquet_files:
        return 0
    for path in parquet_files:
        process_file(path, value_maps, log)
    return len(parquet_files)
