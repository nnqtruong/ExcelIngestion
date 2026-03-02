"""Step 06: Union cleaned Parquet files into one dataset with row_id primary key."""

import logging
import sys
from pathlib import Path

import pandas as pd
import yaml

# Project root (parent of scripts/)
ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
CLEAN_DIR = ROOT / "clean"
ANALYTICS_DIR = ROOT / "analytics"
LOGS_DIR = ROOT / "logs"
COMBINE_PATH = CONFIG_DIR / "combine.yaml"


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


def load_combine_config(path: Path) -> dict:
    """Load combine.yaml. Returns empty dict if file missing."""
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


def get_parquet_files(clean_dir: Path) -> list[Path]:
    """Return sorted list of Parquet paths in clean/ (exclude _errors files)."""
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

    combined = pd.concat(dfs, axis=0, ignore_index=True)
    return combined


def add_row_id(df: pd.DataFrame, log: logging.Logger) -> pd.DataFrame:
    """Add row_id column as first column (1-based integer primary key)."""
    df = df.copy()
    df.insert(0, "row_id", range(1, len(df) + 1))
    log.info("Added row_id primary key (1 to %d)", len(df))
    return df


def main() -> None:
    setup_logging()
    log = logging.getLogger(__name__)

    # Load config (optional - only used for output filename)
    config = load_combine_config(COMBINE_PATH)
    output_name = config.get("output", "combined.parquet")
    output_path = ANALYTICS_DIR / output_name

    # Check input directory
    if not CLEAN_DIR.is_dir():
        log.error("No clean/ directory at %s", CLEAN_DIR)
        sys.exit(1)

    # Get files to combine
    files = get_parquet_files(CLEAN_DIR)
    if not files:
        log.error("No Parquet files to combine in %s", CLEAN_DIR)
        sys.exit(1)

    # Combine all files
    combined = combine_files(files, log)

    # Add row_id as primary key
    combined = add_row_id(combined, log)

    # Write output
    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output_path, index=False)

    log.info("After combine: %s - %d rows", output_path.name, len(combined))
    log.info("Finished combine: output written to %s", output_path)


if __name__ == "__main__":
    main()
