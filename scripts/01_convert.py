"""Step 01: Convert source files (Excel) in raw/ to Parquet in clean/."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.convert import run_convert
from lib.paths import CLEAN_DIR, RAW_DIR


def _print_converted(path: Path, df) -> None:
    """Callback: show file name, row count, and all rows converted."""
    print(f"\n--- Converting: {path.name} -> {path.stem}.parquet ---")
    print(f"Rows converted: {len(df)}")
    print(df.to_string())


if __name__ == "__main__":
    try:
        n = run_convert(RAW_DIR, CLEAN_DIR, on_converted=_print_converted)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Failed to read Excel: {e}", file=sys.stderr)
        sys.exit(1)
    if n:
        print(f"\nDone. Converted {n} file(s).")
    else:
        print("No Excel files in raw/")
