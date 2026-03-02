"""Step 06: Union or join cleaned files per combine.yaml; validate primary key; write to analytics/."""

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
    """Load combine.yaml."""
    if not path.exists():
        raise FileNotFoundError(f"Combine config not found: {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not data:
        raise ValueError(f"combine.yaml is empty: {path}")
    return data


def get_primary_key(config: dict) -> list[str]:
    """Return list of primary key column names."""
    pk = config.get("primary_key")
    if pk is None:
        raise ValueError("combine.yaml must define primary_key")
    return [pk] if isinstance(pk, str) else list(pk)


def get_parquet_files(clean_dir: Path, config: dict) -> list[Path]:
    """Return ordered list of Parquet paths to combine (exclude _errors)."""
    explicit = config.get("files")
    if explicit:
        return [clean_dir / f for f in explicit if f.endswith(".parquet") and not f.endswith("_errors.parquet")]
    all_parquet = sorted(clean_dir.glob("*.parquet"))
    return [p for p in all_parquet if not p.name.endswith("_errors.parquet")]


def validate_no_duplicate_primary_key(df: pd.DataFrame, primary_key: list[str]) -> None:
    """Raise ValueError if any row in primary_key columns is duplicated."""
    missing = [c for c in primary_key if c not in df.columns]
    if missing:
        raise ValueError(f"primary_key columns missing in combined data: {missing}")
    dup = df.duplicated(subset=primary_key, keep=False)
    if dup.any():
        n = dup.sum()
        examples = df.loc[dup, primary_key].drop_duplicates().head()
        raise ValueError(
            f"Duplicate primary key(s) after combination: {n} row(s) involved. Examples:\n{examples}"
        )


def run_union(files: list[Path], config: dict, log: logging.Logger) -> pd.DataFrame:
    """Concat all files; return combined DataFrame. Log row count per file and total before."""
    if not files:
        raise ValueError("No Parquet files to combine")
    dfs = []
    total_before = 0
    for path in files:
        df = pd.read_parquet(path)
        n = len(df)
        total_before += n
        log.info("Before combine: %s - %d rows", path.name, n)
        dfs.append(df)
    combined = pd.concat(dfs, axis=0, ignore_index=True)
    log.info("Before combine: total - %d rows", total_before)
    return combined


def run_join(files: list[Path], config: dict, log: logging.Logger) -> pd.DataFrame:
    """Join files on join_on key; return combined DataFrame. Log row count per file and total before."""
    join_on = config.get("join_on")
    if not join_on:
        raise ValueError("combine.yaml must define join_on for mode=join")
    join_cols = [join_on] if isinstance(join_on, str) else list(join_on)
    if len(files) < 2:
        raise ValueError("join mode requires at least 2 files")
    total_before = 0
    df = pd.read_parquet(files[0])
    n0 = len(df)
    total_before += n0
    log.info("Before combine: %s - %d rows", files[0].name, n0)
    for path in files[1:]:
        other = pd.read_parquet(path)
        n = len(other)
        total_before += n
        log.info("Before combine: %s - %d rows", path.name, n)
        df = df.merge(other, on=join_cols, how="outer")
    log.info("Before combine: total - %d rows", total_before)
    return df


def main() -> None:
    setup_logging()
    log = logging.getLogger(__name__)

    config = load_combine_config(COMBINE_PATH)
    mode = (config.get("mode") or "union").strip().lower()
    primary_key = get_primary_key(config)
    output_name = config.get("output") or "combined.parquet"
    output_path = ANALYTICS_DIR / output_name

    if not CLEAN_DIR.is_dir():
        log.error("No clean/ directory at %s", CLEAN_DIR)
        sys.exit(1)

    files = get_parquet_files(CLEAN_DIR, config)
    if not files:
        log.error("No Parquet files to combine in %s", CLEAN_DIR)
        sys.exit(1)

    if mode == "union":
        combined = run_union(files, config, log)
    elif mode == "join":
        combined = run_join(files, config, log)
    else:
        raise ValueError(f"combine.yaml mode must be 'union' or 'join', got: {mode}")

    validate_no_duplicate_primary_key(combined, primary_key)

    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output_path, index=False)
    after = len(combined)
    log.info("After combine: %s - %d rows", output_path.name, after)
    log.info("Finished combine: output written to %s", output_path)


if __name__ == "__main__":
    main()
