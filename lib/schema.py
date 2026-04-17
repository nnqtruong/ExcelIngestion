"""Load and normalize schema.yaml (list or dict columns)."""
from pathlib import Path

import yaml

from lib.config import load_dataset_config, schema_body_from_merged_config


def load_schema(path: Path) -> dict:
    """Load and return schema from schema.yaml or unified dataset.yaml."""
    dataset_root = path.parent.parent
    if (dataset_root / "dataset.yaml").exists():
        merged = load_dataset_config(dataset_root)
        data = schema_body_from_merged_config(merged)
    else:
        if not path.exists():
            raise FileNotFoundError(f"Schema not found: {path}")
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
    if not data or "columns" not in data:
        raise ValueError(f"Schema must define 'columns': {path}")
    return data


def columns_as_list(schema: dict) -> list[dict]:
    """Return columns as list of {name, dtype?, nullable?, ...} (support list or dict schema)."""
    cols = schema["columns"]
    if isinstance(cols, list):
        return cols
    return [{"name": k, **v} for k, v in cols.items()]


def get_column_order(schema: dict) -> list[str]:
    """Return desired column order from schema."""
    return [c["name"] for c in columns_as_list(schema)]


def get_column_aliases(schema: dict) -> dict[str, str]:
    """Return column alias mapping from schema, or empty dict if not defined."""
    raw = schema.get("column_aliases") or {}
    if not isinstance(raw, dict):
        return {}
    return {str(k): str(v) for k, v in raw.items()}
