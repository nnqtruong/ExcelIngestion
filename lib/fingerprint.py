"""Per-dataset raw file fingerprints for incremental ingestion (MD5 + metadata)."""
import hashlib
import json
from datetime import datetime
from pathlib import Path

STATE_FILE = "_state/ingestion_state.json"
# Written when step 01 skips (no raw changes); step 06 may skip if combined exists.
STEP01_SKIPPED_SENTINEL = "_state/step01_skipped"

_RAW_EXCEL_SUFFIXES = (".xlsx", ".xls")


def touch_step01_skipped_sentinel(dataset_root: Path) -> None:
    """Mark that step 01 did not convert any files (incremental skip)."""
    path = dataset_root / STEP01_SKIPPED_SENTINEL
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("1\n", encoding="utf-8")


def clear_step01_skipped_sentinel(dataset_root: Path) -> None:
    """Remove skip marker (call at start of step 01 so a new run does not inherit a stale flag)."""
    path = dataset_root / STEP01_SKIPPED_SENTINEL
    path.unlink(missing_ok=True)


def prune_orphan_clean_parquets(dataset_root: Path) -> list[Path]:
    """Delete clean/*.parquet with no matching raw Excel (same stem). Excludes *_errors.parquet."""
    raw_dir = dataset_root / "raw"
    clean_dir = dataset_root / "clean"
    if not clean_dir.is_dir():
        return []
    raw_stems: set[str] = set()
    if raw_dir.is_dir():
        for ext in _RAW_EXCEL_SUFFIXES:
            for p in raw_dir.glob(f"*{ext}"):
                raw_stems.add(p.stem)
    removed: list[Path] = []
    for pq in sorted(clean_dir.glob("*.parquet")):
        if pq.name.endswith("_errors.parquet"):
            continue
        if pq.stem not in raw_stems:
            try:
                pq.unlink()
            except OSError:
                continue
            removed.append(pq)
    return removed


def _sorted_raw_excels(raw_dir: Path) -> list[Path]:
    """Match lib/convert.py: both .xlsx and .xls under raw/."""
    files: list[Path] = []
    for ext in _RAW_EXCEL_SUFFIXES:
        files.extend(raw_dir.glob(f"*{ext}"))
    return sorted(files)


def compute_file_hash(filepath: Path) -> str:
    """MD5 hash of file contents. Fast enough for <100MB files."""
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()


def load_state(dataset_root: Path) -> dict:
    """Load ingestion state from JSON. Return empty state if missing."""
    state_path = dataset_root / STATE_FILE
    if state_path.exists():
        with open(state_path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"last_run": None, "files": {}, "combined_row_count": 0}
        data.setdefault("last_run", None)
        data.setdefault("files", {})
        data.setdefault("combined_row_count", 0)
        if not isinstance(data["files"], dict):
            data["files"] = {}
        return data
    return {"last_run": None, "files": {}, "combined_row_count": 0}


def save_state(dataset_root: Path, state: dict) -> None:
    """Save ingestion state to JSON."""
    state_path = dataset_root / STATE_FILE
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state["last_run"] = datetime.now().isoformat()
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def get_changed_files(
    dataset_root: Path, force: bool = False
) -> tuple[list[Path], list[Path], dict]:
    """
    Compare raw/ files against saved state.
    Returns: (new_or_changed_files, unchanged_files, current_state)

    Edge case: If hash matches BUT clean/*.parquet doesn't exist, treat as changed.
    """
    state = load_state(dataset_root)
    raw_dir = dataset_root / "raw"
    clean_dir = dataset_root / "clean"

    prev_files: dict = {}
    for name, meta in state.get("files", {}).items():
        if isinstance(meta, dict):
            prev_files[name] = dict(meta)

    new_or_changed: list[Path] = []
    unchanged: list[Path] = []

    for filepath in _sorted_raw_excels(raw_dir):
        current_hash = compute_file_hash(filepath)
        filename = filepath.name
        expected_parquet = clean_dir / f"{filepath.stem}.parquet"

        meta = state["files"].get(filename)
        if force:
            new_or_changed.append(filepath)
        elif not isinstance(meta, dict):
            new_or_changed.append(filepath)
        elif meta.get("md5") != current_hash:
            new_or_changed.append(filepath)
        elif not expected_parquet.exists():
            new_or_changed.append(filepath)
        else:
            unchanged.append(filepath)

        now = datetime.now().isoformat()
        prev = prev_files.get(filename, {})
        state["files"][filename] = {
            "md5": current_hash,
            "size_bytes": filepath.stat().st_size,
            "first_seen": prev.get("first_seen", now),
            "last_processed": now if filepath in new_or_changed else prev.get("last_processed"),
        }

    current_filenames = {p.name for p in _sorted_raw_excels(raw_dir)}
    for name in list(state["files"].keys()):
        if name not in current_filenames:
            del state["files"][name]

    return new_or_changed, unchanged, state
