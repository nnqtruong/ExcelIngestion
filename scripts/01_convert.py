"""Step 01: Convert source files (Excel) in raw/ to Parquet in clean/."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd


def coerce_mixed_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert object-dtype columns to string to avoid PyArrow mixed-type errors.

    Excel files often have columns with mixed types (e.g., PolicyNumber with both
    strings like 'NPP1234567' and integers like 1234567). PyArrow cannot convert
    these to Parquet, so we coerce them to strings first.
    """
    for col in df.columns:
        if df[col].dtype == "object":
            # Convert to string, but preserve actual NaN/None as pd.NA
            df[col] = df[col].apply(
                lambda x: str(x) if pd.notna(x) and x is not None else pd.NA
            )
    return df

# Project root (parent of scripts/)
ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "raw"
CLEAN_DIR = ROOT / "clean"

EXCEL_EXTENSIONS = (".xlsx", ".xls")


def main() -> None:
    if not RAW_DIR.is_dir():
        print(f"No raw/ directory at {RAW_DIR}", file=sys.stderr)
        sys.exit(1)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    excel_files = []
    for ext in EXCEL_EXTENSIONS:
        excel_files.extend(RAW_DIR.glob(f"*{ext}"))
    excel_files = sorted(excel_files)

    if not excel_files:
        print("No Excel files in raw/")
        return

    for path in excel_files:
        try:
            df = pd.read_excel(path, engine="openpyxl")
        except Exception as e:
            print(f"Failed to read {path.name}: {e}", file=sys.stderr)
            sys.exit(1)

        # Coerce object columns to string to avoid PyArrow mixed-type errors
        df = coerce_mixed_types(df)

        out_path = CLEAN_DIR / f"{path.stem}.parquet"
        df.to_parquet(out_path, index=False)
        print(f"Converted {path.name} -> {out_path.name}")

    print(f"Done. Converted {len(excel_files)} file(s).")


if __name__ == "__main__":
    main()
