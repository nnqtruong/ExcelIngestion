"""Project and dataset path constants. When PIPELINE_DATASET_ROOT is set (by run_pipeline.py), config and data paths are under that directory. Otherwise uses datasets/{env}/{dataset_name} with PIPELINE_ENV (default dev) and PIPELINE_DATASET_NAME (default tasks)."""
import os
import warnings
from pathlib import Path

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
    """Resolve dataset root from PIPELINE_DATASET_ROOT or datasets/{env}/{dataset_name}."""
    explicit = os.environ.get("PIPELINE_DATASET_ROOT")
    if explicit:
        return Path(explicit)
    env = get_env()
    dataset_name = os.environ.get("PIPELINE_DATASET_NAME") or "tasks"
    return ROOT / "datasets" / env / dataset_name


# Dataset root: set by run_pipeline.py via PIPELINE_DATASET_ROOT, or derived from env/dataset_name
DATASET_ROOT = _resolve_dataset_root()

CONFIG_DIR = DATASET_ROOT / "config"
RAW_DIR = DATASET_ROOT / "raw"
CLEAN_DIR = DATASET_ROOT / "clean"
ERRORS_DIR = DATASET_ROOT / "errors"
ANALYTICS_DIR = DATASET_ROOT / "analytics"
LOGS_DIR = DATASET_ROOT / "logs"

SCHEMA_PATH = CONFIG_DIR / "schema.yaml"
COMBINE_PATH = CONFIG_DIR / "combine.yaml"
VALUE_MAPS_PATH = CONFIG_DIR / "value_maps.yaml"
REPORT_PATH = LOGS_DIR / "validation_report.json"
DB_PATH = ANALYTICS_DIR / "tasks.db"
