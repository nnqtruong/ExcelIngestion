"""Step 05: Apply value_maps.yaml to standardize categorical values."""

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
VALUE_MAPS_PATH = CONFIG_DIR / "value_maps.yaml"


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
    """
    Replace values in series using mapping; return (new_series, count_remapped).
    Only counts cells that actually changed (source was non-null and value changed).
    """
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


def main() -> None:
    setup_logging()
    log = logging.getLogger(__name__)

    value_maps = load_value_maps(VALUE_MAPS_PATH)
    if not value_maps:
        log.info("No value maps in %s", VALUE_MAPS_PATH)
        return

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
        process_file(path, value_maps, log)
    log.info("Finished normalizing values for %d file(s).", len(parquet_files))


if __name__ == "__main__":
    main()
