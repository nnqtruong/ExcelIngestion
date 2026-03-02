"""Step 07: Apply null-filling strategies from schema (runs on analytics/ combined output)."""

import logging
import sys
from pathlib import Path

import pandas as pd
import yaml

# Project root (parent of scripts/)
ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
ANALYTICS_DIR = ROOT / "analytics"
LOGS_DIR = ROOT / "logs"
SCHEMA_PATH = CONFIG_DIR / "schema.yaml"
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


def load_schema(path: Path) -> dict:
    """Load schema.yaml."""
    if not path.exists():
        raise FileNotFoundError(f"Schema not found: {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not data or "columns" not in data:
        raise ValueError(f"Schema must define 'columns' list: {path}")
    return data


def get_combined_path() -> Path:
    """Return path to combined Parquet in analytics/ (from combine.yaml or default)."""
    if COMBINE_PATH.exists():
        with open(COMBINE_PATH, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        if config and config.get("output"):
            return ANALYTICS_DIR / config["output"]
    return ANALYTICS_DIR / "combined.parquet"


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


def _columns_as_list(schema: dict) -> list[dict]:
    """Return columns as list of {name, dtype, ...} (support list or dict schema)."""
    cols = schema["columns"]
    if isinstance(cols, list):
        return cols
    return [{"name": k, **v} for k, v in cols.items()]


def process_file(path: Path, schema: dict, log: logging.Logger) -> None:
    """Apply fill strategies per schema; log null rates before and after."""
    df = pd.read_parquet(path)
    n_rows = len(df)

    columns_with_strategy = [
        (c["name"], c["fill_strategy"])
        for c in _columns_as_list(schema)
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
            path.name,
            col,
            strategy,
            before * 100,
            after * 100,
        )

    total_null_after = df.isna().sum().sum() / n_cells if n_cells else 0.0
    log.info(
        "%s: overall null rate before=%.2f%% after=%.2f%%",
        path.name,
        total_null_before * 100,
        total_null_after * 100,
    )
    df.to_parquet(path, index=False)


def main() -> None:
    setup_logging()
    log = logging.getLogger(__name__)

    schema = load_schema(SCHEMA_PATH)
    path = get_combined_path()

    if not path.exists():
        log.error("Combined file not found: %s (run step 06 first)", path)
        sys.exit(1)

    process_file(path, schema, log)
    log.info("Finished null handling for %s", path.name)


if __name__ == "__main__":
    main()
