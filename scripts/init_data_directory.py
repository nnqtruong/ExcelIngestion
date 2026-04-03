"""Initialize external data directory structure."""
from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.data_root import get_data_root

PROJECT_DATASETS = ROOT / "datasets"

DATASETS = [
    "tasks",
    "dept_mapping",
    "employees_master",
    "workers",
    "revenue",
    "launch",
]
SUBDIRS = ["raw", "clean", "errors", "analytics", "logs", "_state", "config"]
ENVS = ["dev", "prod"]


def _rel(p: Path, base: Path) -> str:
    try:
        return str(p.relative_to(base)).replace("\\", "/")
    except ValueError:
        return str(p).replace("\\", "/")


def init_data_directory(
    data_root: Path | None = None,
    *,
    force_config: bool = False,
) -> Path:
    """Create external data directory with proper structure. Safe to run repeatedly.

    - Creates ``{data_root}/{env}/{dataset}/`` subdirs for each env/dataset.
    - Creates ``analytics`` and ``powerbi`` under ``data_root``.
    - Copies YAML config from ``project/datasets/`` into ``data_root`` when a template
      exists; by default skips files that already exist (use ``force_config=True`` to overwrite).
    """
    if data_root is None:
        data_root = get_data_root()
    else:
        data_root = Path(data_root).expanduser().resolve()
        data_root.mkdir(parents=True, exist_ok=True)
        # So lib.data_root helpers match this run if user imports them later in same process
        os.environ["DATA_ROOT"] = str(data_root)

    print(f"Initializing data directory at: {data_root}\n")

    created_dirs: list[str] = []
    for sub in ("analytics", "powerbi"):
        p = data_root / sub
        p.mkdir(parents=True, exist_ok=True)
        created_dirs.append(_rel(p, data_root))

    for env in ENVS:
        for dataset in DATASETS:
            base = data_root / env / dataset
            for name in SUBDIRS:
                d = base / name
                d.mkdir(parents=True, exist_ok=True)
                created_dirs.append(_rel(d, data_root))

    print("Created directories:")
    for line in sorted(set(created_dirs)):
        print(f"  {line}")

    copied: list[str] = []
    skipped: list[str] = []

    for env in ENVS:
        for dataset in DATASETS:
            dst_base = data_root / env / dataset
            src_cfg = PROJECT_DATASETS / env / dataset / "config"
            if src_cfg.is_dir():
                dst_cfg = dst_base / "config"
                dst_cfg.mkdir(parents=True, exist_ok=True)
                for src in sorted(src_cfg.iterdir()):
                    if not src.is_file():
                        continue
                    dst = dst_cfg / src.name
                    if dst.exists() and not force_config:
                        skipped.append(_rel(dst, data_root))
                        continue
                    shutil.copy2(src, dst)
                    copied.append(_rel(dst, data_root))

            src_pl = PROJECT_DATASETS / env / dataset / "pipeline.yaml"
            if src_pl.is_file():
                dst_pl = dst_base / "pipeline.yaml"
                if dst_pl.exists() and not force_config:
                    skipped.append(_rel(dst_pl, data_root))
                else:
                    shutil.copy2(src_pl, dst_pl)
                    copied.append(_rel(dst_pl, data_root))

    print()
    if copied:
        print("Copied config files:")
        for line in sorted(copied):
            print(f"  {line}")
    else:
        print("Copied config files: (none - templates missing or all already present)")

    if skipped and not force_config:
        print()
        print("Skipped (already exists; use --force-config to overwrite):")
        for line in sorted(skipped):
            print(f"  {line}")

    print()
    print("Data directory initialized successfully!")
    print()
    print("Next steps:")
    dr = data_root
    print(f"1. Copy your Excel files to: {dr}\\dev\\{{dataset}}\\raw\\")
    print("2. Run: python run_pipeline.py --dataset {dataset}")
    print()
    print("Replace {dataset} with one of: " + ", ".join(DATASETS))

    return data_root


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create external data folder layout and seed config YAML from project templates."
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="Override data root (default: DATA_ROOT env or ../ExcelIngestion_Data)",
    )
    parser.add_argument(
        "--force-config",
        action="store_true",
        help="Overwrite existing schema.yaml, combine.yaml, pipeline.yaml, etc.",
    )
    args = parser.parse_args()
    init_data_directory(args.data_root, force_config=args.force_config)
