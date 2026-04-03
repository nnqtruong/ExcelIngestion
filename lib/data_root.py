"""External data directory resolution for persistent user data across updates."""
import logging
import os
from pathlib import Path

# Same project root as lib/paths.ROOT — kept local to avoid import side effects.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent

_logger = logging.getLogger(__name__)

_data_root_logged = False


def get_data_root() -> Path:
    """
    Get the external data root directory.

    Priority:
    1. DATA_ROOT environment variable
    2. Default: ../ExcelIngestion_Data (sibling to project)

    Creates the directory if it doesn't exist.
    """
    global _data_root_logged
    explicit = os.environ.get("DATA_ROOT")
    if explicit and explicit.strip():
        root = Path(explicit.strip()).expanduser().resolve()
    else:
        root = (_PROJECT_ROOT.parent / "ExcelIngestion_Data").resolve()

    root.mkdir(parents=True, exist_ok=True)

    if not _data_root_logged:
        _logger.info("Using data root: %s", root)
        _data_root_logged = True

    return root


def get_dataset_path(env: str, dataset: str) -> Path:
    """Get path for a specific dataset: {data_root}/{env}/{dataset}/"""
    return get_data_root() / env / dataset


def get_analytics_path() -> Path:
    """Get path for SQLite databases: {data_root}/analytics/"""
    return get_data_root() / "analytics"


def get_powerbi_path() -> Path:
    """Get path for DuckDB files: {data_root}/powerbi/"""
    return get_data_root() / "powerbi"
