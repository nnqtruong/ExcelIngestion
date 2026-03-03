"""Project and dataset path constants. When PIPELINE_DATASET_ROOT is set (by run_pipeline.py), config and data paths are under that directory."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Dataset root: set by run_pipeline.py via env when reading datasets/tasks/pipeline.yaml
_DATASET_ROOT = os.environ.get("PIPELINE_DATASET_ROOT")
DATASET_ROOT = Path(_DATASET_ROOT) if _DATASET_ROOT else ROOT

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
