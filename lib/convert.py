"""Step 01: Convert Excel in raw/ to Parquet in clean/."""
from pathlib import Path
from typing import Callable

import pandas as pd

EXCEL_EXTENSIONS = (".xlsx", ".xls")


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
    on_converted: Callable[[Path, "pd.DataFrame"], None] | None = None,
) -> int:
    """Read all Excel from raw_dir, write Parquet to clean_dir. Returns number of files converted.
    If on_converted(path, df) is given, call it after each file is converted (for logging/printing)."""
    if not raw_dir.is_dir():
        raise FileNotFoundError(f"No raw/ directory at {raw_dir}")
    clean_dir.mkdir(parents=True, exist_ok=True)

    excel_files = []
    for ext in EXCEL_EXTENSIONS:
        excel_files.extend(raw_dir.glob(f"*{ext}"))
    excel_files = sorted(excel_files)

    if not excel_files:
        return 0

    for path in excel_files:
        df = pd.read_excel(path, engine="openpyxl")
        df = coerce_mixed_types(df)
        out_path = clean_dir / f"{path.stem}.parquet"
        df.to_parquet(out_path, index=False)
        if on_converted is not None:
            on_converted(path, df)
    return len(excel_files)
