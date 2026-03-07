"""
Diagnostic script: verify Power BI can consume powerbi/warehouse.duckdb.
Lists tables/views, inspects report_* tables, runs a sample query, exports to sample_output.csv.
"""
from pathlib import Path

import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
DUCKDB_PATH = SCRIPT_DIR / "warehouse.duckdb"
SAMPLE_OUTPUT_PATH = SCRIPT_DIR / "sample_output.csv"


def main() -> None:
    if not DUCKDB_PATH.exists():
        print(f"ERROR: Database not found: {DUCKDB_PATH}")
        print("Run powerbi/refresh.py first.")
        return

    # DuckDB version and file size
    print(f"DuckDB version: {duckdb.__version__}")
    size_mb = DUCKDB_PATH.stat().st_size / (1024 * 1024)
    print(f"File size: {size_mb:.2f} MB")
    print(f"Database: {DUCKDB_PATH}\n")

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    # List all tables and views
    tables = conn.execute("""
        SELECT table_name, table_type FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_type, table_name
    """).fetchall()
    print("Tables and views:")
    for name, ttype in tables:
        print(f"  [{ttype}] {name}")
    print()

    # For each report_* table: name, row count, column names, first 3 rows
    report_tables = [row[0] for row in tables if row[0].startswith("report_")]
    for name in sorted(report_tables):
        n = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        cols = [row[0] for row in conn.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position", [name]).fetchall()]
        print(f"--- {name} ---")
        print(f"  Rows: {n}")
        print(f"  Columns: {cols}")
        rows = conn.execute(f"SELECT * FROM {name} LIMIT 3").fetchall()
        print(f"  First 3 rows:")
        for i, r in enumerate(rows, 1):
            print(f"    {i}: {r}")
        print()

    # Sample query (as Power BI would use)
    sample_sql = """
        SELECT task_date, COUNT(*) AS tasks, AVG(duration_hours) AS avg_hours
        FROM report_tasks_full
        WHERE task_date IS NOT NULL
        GROUP BY task_date
        ORDER BY task_date
    """
    print("Sample query (Power BI–style):")
    print(sample_sql.strip())
    result = conn.execute(sample_sql).fetchdf()
    print(f"\nResult: {len(result)} rows")
    print(result.to_string())
    print()

    # Export to CSV
    result.to_csv(SAMPLE_OUTPUT_PATH, index=False)
    print(f"Exported to: {SAMPLE_OUTPUT_PATH}")

    conn.close()
    print("\nConnect test done.")


if __name__ == "__main__":
    main()
