"""Load combine.yaml, pipeline.yaml, and resolve paths."""
import os
from pathlib import Path

import yaml

from lib.data_root import get_analytics_path


def load_pipeline_config(dataset_root: Path) -> dict:
    """Load pipeline.yaml from dataset root. Returns empty dict if missing."""
    path = dataset_root / "pipeline.yaml"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


def get_sqlite_config(dataset_root: Path) -> dict:
    """Return sqlite section from pipeline.yaml. Default database and table if missing."""
    pipeline = load_pipeline_config(dataset_root)
    sqlite = pipeline.get("sqlite") or {}
    return {
        "database": sqlite.get("database", "tasks.db"),
        "table": sqlite.get("table", "tasks"),
    }


def get_sqlite_table_name(dataset_root: Path) -> str:
    """Return table name for SQLite export from pipeline.yaml."""
    return get_sqlite_config(dataset_root)["table"]


def get_sqlite_db_path(dataset_root: Path) -> Path:
    """Path to SQLite DB. Uses PIPELINE_ENV (default: dev). Dev: dev_warehouse.db, prod: warehouse.db when pipeline uses shared warehouse name; otherwise {database} under external analytics dir."""
    cfg = get_sqlite_config(dataset_root)
    db_name = cfg["database"]
    env = os.environ.get("PIPELINE_ENV") or "dev"
    base = get_analytics_path()
    if db_name == "warehouse.db":
        shared_name = "dev_warehouse.db" if env == "dev" else "warehouse.db"
        return base / shared_name
    return base / db_name


def load_combine_config(path: Path) -> dict:
    """Load combine.yaml. Returns empty dict if file missing."""
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


def get_combined_path(analytics_dir: Path, combine_config_path: Path) -> Path:
    """Return path to combined Parquet (from combine.yaml output or default)."""
    config = load_combine_config(combine_config_path)
    output_name = config.get("output", "combined.parquet") if config else "combined.parquet"
    return analytics_dir / output_name
