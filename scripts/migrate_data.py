"""Migrate pipeline data from in-repo datasets/ (and legacy analytics/, powerbi/) to external data root."""
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


def _sqlite_sidecars(db_file: Path) -> list[Path]:
    """Optional SQLite journal files next to a .db file."""
    out: list[Path] = []
    for suffix in ("-wal", "-shm", "-journal"):
        p = db_file.parent / f"{db_file.name}{suffix}"
        if p.is_file():
            out.append(p)
    return out


def _duckdb_sidecars(duckdb_file: Path) -> list[Path]:
    """Optional DuckDB WAL next to a .duckdb file."""
    wal = Path(str(duckdb_file) + ".wal")
    if wal.is_file():
        return [wal]
    return []


def _collect_dataset_file_moves(project_root: Path, data_root: Path) -> list[tuple[Path, Path]]:
    """Build (src, dst) pairs for datasets/{env}/{dataset}/... -> external mirror."""
    jobs: list[tuple[Path, Path]] = []
    datasets_root = project_root / "datasets"
    if not datasets_root.is_dir():
        return jobs

    for env_dir in sorted(datasets_root.iterdir()):
        if not env_dir.is_dir() or env_dir.name.startswith("."):
            continue
        env = env_dir.name
        for ds_dir in sorted(env_dir.iterdir()):
            if not ds_dir.is_dir():
                continue
            dataset = ds_dir.name
            dest_base = data_root / env / dataset

            raw = ds_dir / "raw"
            if raw.is_dir():
                for f in sorted(raw.glob("*.xlsx")):
                    if f.is_file():
                        jobs.append((f, dest_base / "raw" / f.name))

            clean = ds_dir / "clean"
            if clean.is_dir():
                for f in sorted(clean.glob("*.parquet")):
                    if f.is_file():
                        jobs.append((f, dest_base / "clean" / f.name))

            combined = ds_dir / "analytics" / "combined.parquet"
            if combined.is_file():
                jobs.append((combined, dest_base / "analytics" / "combined.parquet"))

            state = ds_dir / "_state"
            if state.is_dir():
                for f in sorted(state.iterdir()):
                    if f.is_file():
                        jobs.append((f, dest_base / "_state" / f.name))

            logs = ds_dir / "logs"
            if logs.is_dir():
                for f in sorted(logs.iterdir()):
                    if f.is_file():
                        jobs.append((f, dest_base / "logs" / f.name))

    return jobs


def _collect_root_warehouse_moves(project_root: Path, data_root: Path) -> list[tuple[Path, Path]]:
    """Legacy project-root analytics/*.db and powerbi/*.duckdb -> external analytics/ and powerbi/."""
    jobs: list[tuple[Path, Path]] = []

    analytics = project_root / "analytics"
    if analytics.is_dir():
        dest_dir = data_root / "analytics"
        for f in sorted(analytics.glob("*.db")):
            if not f.is_file():
                continue
            jobs.append((f, dest_dir / f.name))
            for extra in _sqlite_sidecars(f):
                jobs.append((extra, dest_dir / extra.name))

    powerbi_dir = project_root / "powerbi"
    if powerbi_dir.is_dir():
        dest_dir = data_root / "powerbi"
        for f in sorted(powerbi_dir.glob("*.duckdb")):
            if not f.is_file():
                continue
            jobs.append((f, dest_dir / f.name))
            for extra in _duckdb_sidecars(f):
                jobs.append((extra, dest_dir / extra.name))

    return jobs


def migrate_data(
    *,
    data_root: Path | None = None,
    dry_run: bool = False,
    overwrite: bool = False,
) -> dict[str, int]:
    """
    Move files from project ``datasets/`` (and root ``analytics/``, ``powerbi/``) into ``data_root``.

    Returns counts: moved, skipped_conflict, skipped_missing, failed.
    """
    project_root = ROOT
    if data_root is None:
        data_root = get_data_root()
    else:
        data_root = Path(data_root).expanduser().resolve()
        data_root.mkdir(parents=True, exist_ok=True)
        os.environ["DATA_ROOT"] = str(data_root)

    all_jobs = _collect_dataset_file_moves(project_root, data_root) + _collect_root_warehouse_moves(
        project_root, data_root
    )

    # Deduplicate while preserving order (same dst shouldn't appear twice)
    seen_dst: set[Path] = set()
    jobs: list[tuple[Path, Path]] = []
    for src, dst in all_jobs:
        if not src.is_file():
            continue
        key = dst.resolve()
        if key in seen_dst:
            continue
        seen_dst.add(key)
        jobs.append((src, dst))

    stats = {"moved": 0, "skipped_conflict": 0, "skipped_missing": 0, "failed": 0}

    if not jobs:
        print("No files matched migration rules under the old in-repo locations.")
        print("  (Expected: datasets/{env}/{dataset}/raw/*.xlsx, clean/*.parquet, ...)")
        print("  Nothing to do.")
        return stats

    mode = "DRY RUN - no files will be moved" if dry_run else "MIGRATING"
    print(f"{mode}")
    print(f"  Source project: {project_root}")
    print(f"  Destination:    {data_root}\n")

    for src, dst in jobs:
        rel_src = src.relative_to(project_root) if src.is_relative_to(project_root) else src
        if not src.is_file():
            stats["skipped_missing"] += 1
            print(f"  [skip missing] {rel_src}")
            continue

        dst_parent = dst.parent
        if dst.exists() or dst.is_symlink():
            if overwrite:
                if not dry_run:
                    dst.unlink()
            else:
                stats["skipped_conflict"] += 1
                print(f"  [skip exists]  {rel_src} -> {dst.relative_to(data_root) if dst.is_relative_to(data_root) else dst}")
                continue

        print(f"  {'Would move' if dry_run else 'Move'}: {rel_src} -> {dst.relative_to(data_root) if dst.is_relative_to(data_root) else dst}")

        if dry_run:
            stats["moved"] += 1
            continue

        try:
            dst_parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst))
            stats["moved"] += 1
        except OSError as e:
            stats["failed"] += 1
            print(f"  [ERROR] {rel_src}: {e}")

    print()
    print("Summary")
    print(f"  {'Would move' if dry_run else 'Moved'}:           {stats['moved']}")
    print(f"  Skipped (destination exists): {stats['skipped_conflict']}")
    print(f"  Skipped (source missing):     {stats['skipped_missing']}")
    print(f"  Failed:                       {stats['failed']}")

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Move data from repo datasets/ and legacy analytics/ and powerbi/ into the external DATA_ROOT."
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="External data root (default: DATA_ROOT or ../ExcelIngestion_Data)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List moves only; do not create dirs or move files",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace files that already exist at the destination",
    )
    args = parser.parse_args()
    migrate_data(data_root=args.data_root, dry_run=args.dry_run, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
