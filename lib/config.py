"""Load combine.yaml, pipeline.yaml, dataset.yaml (unified), and resolve paths."""
import os
from pathlib import Path

import yaml

from lib.data_root import get_analytics_path

# Top-level keys that come from pipeline.yaml (split layout) or the pipeline section of dataset.yaml.
PIPELINE_ROOT_KEYS = frozenset(
    {"name", "dataset", "environment", "description", "steps", "sqlite"},
)


def schema_body_from_merged_config(merged: dict) -> dict:
    """Schema-shaped dict: merged config minus pipeline roots and per-file aux blocks."""
    return {
        k: v
        for k, v in merged.items()
        if k not in PIPELINE_ROOT_KEYS and k not in ("combine", "value_maps")
    }


def load_dataset_config(dataset_root: Path) -> dict:
    """
    Load unified dataset.yaml if present.
    Falls back to merging legacy split files (pipeline.yaml, config/*.yaml).

    Returns dict with keys used by loaders, including: name, dataset, environment,
    description, steps, sqlite, combine, columns, column_order, column_aliases,
    validation, value_maps (as present in files).
    """
    unified = dataset_root / "dataset.yaml"
    if unified.exists():
        with open(unified, encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        for key, empty in (
            ("columns", {}),
            ("column_order", []),
            ("column_aliases", {}),
            ("validation", {}),
            ("value_maps", {}),
            ("combine", {}),
        ):
            if key not in config or config.get(key) is None:
                config[key] = empty
        return config

    config: dict = {}

    pipeline_path = dataset_root / "pipeline.yaml"
    if pipeline_path.exists():
        with open(pipeline_path, encoding="utf-8") as f:
            pipeline_data = yaml.safe_load(f) or {}
            config.update(pipeline_data)

    schema_path = dataset_root / "config" / "schema.yaml"
    if schema_path.exists():
        with open(schema_path, encoding="utf-8") as f:
            schema = yaml.safe_load(f) or {}
            config["columns"] = schema.get("columns", {})
            config["column_order"] = schema.get("column_order", [])
            config["column_aliases"] = schema.get("column_aliases", {})
            config["validation"] = schema.get("validation", {})

    value_maps_path = dataset_root / "config" / "value_maps.yaml"
    if value_maps_path.exists():
        with open(value_maps_path, encoding="utf-8") as f:
            config["value_maps"] = yaml.safe_load(f) or {}
    else:
        config["value_maps"] = {}

    combine_path = dataset_root / "config" / "combine.yaml"
    if combine_path.exists():
        with open(combine_path, encoding="utf-8") as f:
            config["combine"] = yaml.safe_load(f) or {}
    else:
        config["combine"] = {}

    return config


def load_pipeline_config(dataset_root: Path) -> dict:
    """Load pipeline.yaml settings, or the same keys from unified dataset.yaml / merged legacy."""
    merged = load_dataset_config(dataset_root)
    return {k: v for k, v in merged.items() if k in PIPELINE_ROOT_KEYS}


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
    """Load combine.yaml, or the combine block from unified dataset.yaml."""
    dataset_root = path.parent.parent
    if (dataset_root / "dataset.yaml").exists():
        return load_dataset_config(dataset_root).get("combine") or {}
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


def load_value_maps(path: Path) -> dict:
    """Load value_maps.yaml, or the value_maps block from unified dataset.yaml."""
    dataset_root = path.parent.parent
    if (dataset_root / "dataset.yaml").exists():
        raw = load_dataset_config(dataset_root).get("value_maps")
    elif not path.exists():
        return {}
    else:
        with open(path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        if raw is None:
            return {}
    if not isinstance(raw, dict):
        raise ValueError(f"value_maps.yaml must be a dict (column -> map): {path}")
    return raw


def get_combined_path(analytics_dir: Path, combine_config_path: Path) -> Path:
    """Return path to combined Parquet (from combine.yaml output or default)."""
    config = load_combine_config(combine_config_path)
    output_name = config.get("output", "combined.parquet") if config else "combined.parquet"
    return analytics_dir / output_name
