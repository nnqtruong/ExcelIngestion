"""
Single command to rebuild from raw Excel to Power BI–ready DuckDB.
Run from project root: python powerbi/refresh.py [--skip-pipeline] [--verbose]
"""
import argparse
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
DUCKDB_PATH = SCRIPT_DIR / "warehouse.duckdb"


def run_cmd(args: list[str], step_name: str) -> bool:
    """Run command from project root. Return True if exit code 0."""
    try:
        result = subprocess.run(
            [sys.executable] + args,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=600,
        )
    except subprocess.TimeoutExpired:
        print(f"Step failed (timeout): {step_name}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Step failed: {step_name}: {e}", file=sys.stderr)
        return False
    if result.returncode != 0:
        print(f"Step failed: {step_name}", file=sys.stderr)
        if result.stdout:
            print(result.stdout, file=sys.stderr)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        return False
    return True


def print_summary(skip_pipeline: bool, verbose: bool) -> None:
    """Print final summary and optionally row counts."""
    if not DUCKDB_PATH.exists():
        print("Database not found; cannot print summary.", file=sys.stderr)
        return
    size_mb = DUCKDB_PATH.stat().st_size / (1024 * 1024)

    import duckdb
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    tables = [
        row[0] for row in conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """).fetchall()
    ]
    views = [
        row[0] for row in conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main' AND table_type = 'VIEW'
            ORDER BY table_name
        """).fetchall()
    ]

    print("\n=== REFRESH COMPLETE ===")
    if not skip_pipeline:
        print("Tasks pipeline:     OK")
        print("Dept mapping:       OK")
    else:
        print("Tasks pipeline:     (skipped)")
        print("Dept mapping:       (skipped)")
    print("DuckDB views:       OK")
    print("Report tables:      OK")
    print(f"Database: powerbi/warehouse.duckdb")
    print(f"Size: {size_mb:.1f} MB")
    print(f"Tables: {tables}")
    print(f"Views: {views}")

    if verbose:
        print("\nRow counts:")
        for name in tables + views:
            n = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            print(f"  {name}: {n} rows")
    conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rebuild from raw Excel to Power BI–ready DuckDB.",
    )
    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="Skip pipeline steps; only rebuild DuckDB from existing Parquet.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print row counts for every table and view.",
    )
    args = parser.parse_args()

    if not args.skip_pipeline:
        if not run_cmd(["run_pipeline.py", "--dataset", "tasks"], "Tasks pipeline"):
            return 1
        if not run_cmd(["run_pipeline.py", "--dataset", "dept_mapping"], "Dept mapping"):
            return 1

    if not run_cmd(["powerbi/create_views.py"], "DuckDB views"):
        return 1
    if not run_cmd(["powerbi/create_report_tables.py"], "Report tables"):
        return 1

    print_summary(args.skip_pipeline, args.verbose)
    return 0


if __name__ == "__main__":
    sys.exit(main())
