"""Project and dataset path constants. When PIPELINE_DATASET_ROOT is set (by run_pipeline.py), config and data paths are under that directory. Otherwise uses {DATA_ROOT}/{env}/{dataset_name} (default DATA_ROOT: sibling ExcelIngestion_Data) with PIPELINE_ENV (default dev) and PIPELINE_DATASET_NAME (default tasks)."""
import os
import warnings
from pathlib import Path

from lib.data_root import (
    get_analytics_path,
    get_data_root,
    get_dataset_path,
    get_powerbi_path,
)

ROOT = Path(__file__).resolve().parent.parent

_ENV_WARNED = False


def get_env() -> str:
    """Return current pipeline environment (PIPELINE_ENV). Defaults to 'dev' if unset."""
    global _ENV_WARNED
    env = os.environ.get("PIPELINE_ENV")
    if env is None or env == "":
        if not _ENV_WARNED:
            warnings.warn(
                "PIPELINE_ENV is not set; defaulting to 'dev'. Set PIPELINE_ENV (e.g. 'dev' or 'prod') to silence.",
                UserWarning,
                stacklevel=2,
            )
            _ENV_WARNED = True
        return "dev"
    return env


def _resolve_dataset_root() -> Path:
    """Resolve dataset root from PIPELINE_DATASET_ROOT or get_dataset_path(env, dataset_name)."""
    explicit = os.environ.get("PIPELINE_DATASET_ROOT")
    if explicit:
        return Path(explicit)
    env = get_env()
    dataset_name = os.environ.get("PIPELINE_DATASET_NAME") or "tasks"
    return get_dataset_path(env, dataset_name)


def get_dataset_root() -> Path:
    """Return the active dataset root (same rules as DATASET_ROOT)."""
    return _resolve_dataset_root()


def get_raw_path(env: str, dataset: str) -> Path:
    """Path to raw inputs: {data_root}/{env}/{dataset}/raw"""
    return get_dataset_path(env, dataset) / "raw"


def get_clean_path(env: str, dataset: str) -> Path:
    """Path to clean Parquet: {data_root}/{env}/{dataset}/clean"""
    return get_dataset_path(env, dataset) / "clean"


def get_sqlite_path() -> Path:
    """Path to the SQLite warehouse file for the current dataset (see lib.config.get_sqlite_db_path)."""
    from lib.config import get_sqlite_db_path

    return get_sqlite_db_path(get_dataset_root())


def get_duckdb_path() -> Path:
    """Path to the DuckDB warehouse file for Power BI (dev: dev_warehouse.duckdb, prod: warehouse.duckdb)."""
    env = os.environ.get("PIPELINE_ENV") or "dev"
    name = "dev_warehouse.duckdb" if env == "dev" else "warehouse.duckdb"
    return get_powerbi_path() / name


# Dataset root: set by run_pipeline.py via PIPELINE_DATASET_ROOT, or derived from env/dataset_name
DATASET_ROOT = _resolve_dataset_root()

CONFIG_DIR = DATASET_ROOT / "config"
RAW_DIR = DATASET_ROOT / "raw"
CLEAN_DIR = DATASET_ROOT / "clean"
ERRORS_DIR = DATASET_ROOT / "errors"
# Per-dataset combined Parquet (steps 06–09); SQLite/DuckDB warehouses stay under get_analytics_path()/get_powerbi_path()
ANALYTICS_DIR = DATASET_ROOT / "analytics"
LOGS_DIR = DATASET_ROOT / "logs"

SCHEMA_PATH = CONFIG_DIR / "schema.yaml"
COMBINE_PATH = CONFIG_DIR / "combine.yaml"
VALUE_MAPS_PATH = CONFIG_DIR / "value_maps.yaml"
REPORT_PATH = LOGS_DIR / "validation_report.json"
DB_PATH = get_analytics_path() / "dev_warehouse.db"
