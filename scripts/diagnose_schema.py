"""Diagnostic script: Compare Parquet schemas in clean/ to detect mismatches."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb

# Configuration
DATASET = "tasks"
ENV = "dev"
CLEAN_DIR = ROOT / "datasets" / ENV / DATASET / "clean"


def get_parquet_schema(conn: duckdb.DuckDBPyConnection, path: Path) -> dict:
    """Get column names and dtypes from a Parquet file."""
    path_sql = str(path.resolve().as_posix()).replace("'", "''")
    cur = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{path_sql}')")
    rows = cur.fetchall()
    # Returns list of (column_name, column_type, null, key, default, extra)
    return {row[0]: row[1] for row in rows}


def get_row_count(conn: duckdb.DuckDBPyConnection, path: Path) -> int:
    """Get row count from a Parquet file."""
    path_sql = str(path.resolve().as_posix()).replace("'", "''")
    return conn.execute(f"SELECT COUNT(*) FROM read_parquet('{path_sql}')").fetchone()[0]


def compare_schemas(baseline: dict, other: dict) -> dict:
    """Compare two schemas. Returns dict with extra, missing, type_mismatch."""
    baseline_cols = set(baseline.keys())
    other_cols = set(other.keys())

    extra = {col: other[col] for col in (other_cols - baseline_cols)}
    missing = {col: baseline[col] for col in (baseline_cols - other_cols)}

    type_mismatch = {}
    for col in baseline_cols & other_cols:
        if baseline[col] != other[col]:
            type_mismatch[col] = {"baseline": baseline[col], "this_file": other[col]}

    return {
        "extra": extra,
        "missing": missing,
        "type_mismatch": type_mismatch,
    }


def main():
    print("=" * 60)
    print("=== Schema Diagnostic Report ===")
    print("=" * 60)
    print(f"Clean directory: {CLEAN_DIR}")
    print()

    if not CLEAN_DIR.is_dir():
        print(f"ERROR: Directory not found: {CLEAN_DIR}")
        return 1

    # Get all parquet files (exclude errors and tmp)
    parquet_files = sorted([
        p for p in CLEAN_DIR.glob("*.parquet")
        if not p.name.endswith("_errors.parquet") and not p.name.startswith("tmp")
    ])

    if not parquet_files:
        print("ERROR: No Parquet files found")
        return 1

    print(f"Found {len(parquet_files)} Parquet files\n")

    conn = duckdb.connect()

    # Get baseline schema from first file
    baseline_file = parquet_files[0]
    baseline_schema = get_parquet_schema(conn, baseline_file)
    baseline_rows = get_row_count(conn, baseline_file)

    print(f"Baseline: {baseline_file.name} ({len(baseline_schema)} columns, {baseline_rows:,} rows)")
    print(f"Columns: {list(baseline_schema.keys())}")
    print()

    # Compare each file
    matches = []
    mismatches = []
    all_mismatched_cols = set()

    file_details = []

    for pf in parquet_files:
        schema = get_parquet_schema(conn, pf)
        rows = get_row_count(conn, pf)

        file_info = {
            "name": pf.name,
            "columns": len(schema),
            "rows": rows,
            "schema": schema,
        }

        if pf == baseline_file:
            file_info["status"] = "baseline"
            file_details.append(file_info)
            continue

        diff = compare_schemas(baseline_schema, schema)
        total_diffs = len(diff["extra"]) + len(diff["missing"]) + len(diff["type_mismatch"])

        if total_diffs == 0:
            file_info["status"] = "match"
            matches.append(pf.name)
            print(f"[OK] {pf.name} - matches baseline ({rows:,} rows)")
        else:
            file_info["status"] = "mismatch"
            file_info["diff"] = diff
            mismatches.append(pf.name)

            print(f"[MISMATCH] {pf.name} - {total_diffs} difference(s) ({rows:,} rows):")

            for col, dtype in diff["extra"].items():
                print(f"   EXTRA COLUMN:   {col} ({dtype})")
                all_mismatched_cols.add(col)

            for col, dtype in diff["missing"].items():
                print(f"   MISSING COLUMN: {col} ({dtype})")
                all_mismatched_cols.add(col)

            for col, types in diff["type_mismatch"].items():
                print(f"   TYPE MISMATCH:  {col} — baseline: {types['baseline']}, this file: {types['this_file']}")
                all_mismatched_cols.add(col)

        file_details.append(file_info)

    conn.close()

    # Summary
    print()
    print("=" * 60)
    print("=== Summary ===")
    print("=" * 60)
    total = len(parquet_files)
    match_count = len(matches) + 1  # +1 for baseline
    print(f"{match_count}/{total} files match baseline schema")
    print(f"{len(mismatches)}/{total} files have mismatches")

    if all_mismatched_cols:
        print(f"\nAll mismatched columns: {', '.join(sorted(all_mismatched_cols))}")

    # Detailed column comparison table
    print()
    print("=" * 60)
    print("=== Column Count by File ===")
    print("=" * 60)
    for info in file_details:
        status_icon = "[BASE]" if info["status"] == "baseline" else ("[OK]" if info["status"] == "match" else "[MISMATCH]")
        print(f"{status_icon} {info['name']}: {info['columns']} columns, {info['rows']:,} rows")

    # Show schema of mismatched files in detail
    if mismatches:
        print()
        print("=" * 60)
        print("=== Detailed Schema of Mismatched Files ===")
        print("=" * 60)
        for info in file_details:
            if info.get("status") == "mismatch":
                print(f"\n{info['name']}:")
                print(f"  Columns ({info['columns']}): {list(info['schema'].keys())}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
