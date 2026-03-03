"""Step 01: Convert Excel in raw/ to Parquet in clean/."""
import sys
import time
from pathlib import Path
from typing import Callable

import pandas as pd

EXCEL_EXTENSIONS = (".xlsx", ".xls")


def _format_size(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"


def _format_time(seconds: float) -> str:
    """Format seconds as human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    else:
        mins = int(seconds // 60)
        secs = seconds % 60
        return f"{mins}m {secs:.1f}s"


def coerce_mixed_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert object-dtype columns to string to avoid PyArrow mixed-type errors."""
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: str(x) if pd.notna(x) and x is not None else pd.NA
            )
    return df


def run_convert(
    raw_dir: Path,
    clean_dir: Path,
    on_progress: Callable[[int, int, Path, int, float], None] | None = None,
    verbose: bool = True,
) -> int:
    """Read all Excel from raw_dir, write Parquet to clean_dir. Returns number of files converted.

    Args:
        raw_dir: Directory containing Excel files
        clean_dir: Directory to write Parquet files
        on_progress: Callback(current, total, path, rows, elapsed_seconds) after each file
        verbose: If True and no callback, print progress to stdout
    """
    if not raw_dir.is_dir():
        raise FileNotFoundError(f"No raw/ directory at {raw_dir}")
    clean_dir.mkdir(parents=True, exist_ok=True)

    excel_files = []
    for ext in EXCEL_EXTENSIONS:
        excel_files.extend(raw_dir.glob(f"*{ext}"))
    excel_files = sorted(excel_files)

    if not excel_files:
        return 0

    total = len(excel_files)
    total_size = sum(f.stat().st_size for f in excel_files)
    total_rows = 0
    start_time = time.time()

    if verbose and on_progress is None:
        print(f"Converting {total} Excel file(s) ({_format_size(total_size)})...")
        sys.stdout.flush()

    for i, path in enumerate(excel_files, 1):
        file_start = time.time()
        file_size = path.stat().st_size

        if verbose and on_progress is None:
            print(f"  [{i}/{total}] {path.name} ({_format_size(file_size)})... ", end="")
            sys.stdout.flush()

        df = pd.read_excel(path, engine="openpyxl")
        df = coerce_mixed_types(df)
        out_path = clean_dir / f"{path.stem}.parquet"
        df.to_parquet(out_path, index=False)

        file_elapsed = time.time() - file_start
        total_rows += len(df)

        if on_progress is not None:
            on_progress(i, total, path, len(df), file_elapsed)
        elif verbose:
            print(f"{len(df):,} rows in {_format_time(file_elapsed)}")
            sys.stdout.flush()

    total_elapsed = time.time() - start_time
    if verbose and on_progress is None:
        print(f"Done. {total_rows:,} total rows in {_format_time(total_elapsed)}")

    return total
