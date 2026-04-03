"""Compare dbt mart_tasks_enriched (DuckDB) with pipeline Parquet output.
Run from repo root: python tests/compare_dbt_vs_pipeline.py

Uses DATA_ROOT (default: sibling ExcelIngestion_Data) for DuckDB and tasks combined Parquet.
"""
import os
import sys
from pathlib import Path

import duckdb

# Repo root (parent of tests/)
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.data_root import get_data_root, get_powerbi_path

DBT_CRC = ROOT / "dbt_crc"
DUCKDB_PATH = get_powerbi_path() / "dev_warehouse.duckdb"
PARQUET_PATH = get_data_root() / "dev" / "tasks" / "analytics" / "combined.parquet"

# Columns to compare null rates (must exist in both; mart has normalized taskstatus/flowname)
KEY_COLUMNS = ["row_id", "taskid", "taskstatus", "flowname", "drawer", "starttime", "endtime"]


def main() -> None:
    if not DUCKDB_PATH.exists():
        print(f"FAIL: DuckDB not found: {DUCKDB_PATH}")
        return
    if not PARQUET_PATH.exists():
        print(f"FAIL: Parquet not found: {PARQUET_PATH}")
        return

    prev_cwd = os.getcwd()
    os.chdir(DBT_CRC)
    con = duckdb.connect(str(DUCKDB_PATH.resolve()), read_only=True)
    parquet_path_str = str(PARQUET_PATH.resolve())

    results = {"row_count": None, "columns": None, "null_rates": None, "taskstatus_flowname": None}
    failures = []
    mart_count = parquet_count = 0
    common_columns = []
    key_in_both = []
    null_rates_mart = {}
    null_rates_parquet = {}
    sample_cols = []
    value_counts = {}

    try:
        # --- Row counts ---
        mart_count = con.execute("SELECT count(*) FROM main.mart_tasks_enriched").fetchone()[0]
        parquet_count = con.execute(
            "SELECT count(*) FROM read_parquet(?)", [parquet_path_str]
        ).fetchone()[0]
        results["row_count"] = (mart_count, parquet_count)
        if mart_count != parquet_count:
            failures.append(
                f"Row count: mart={mart_count}, parquet={parquet_count} (must be equal)"
            )

        # --- Column names present in both ---
        mart_columns = set(
            row[0]
            for row in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'main' AND table_name = 'mart_tasks_enriched'"
            ).fetchall()
        )
        cur = con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [parquet_path_str])
        parquet_columns = {d[0] for d in cur.description}
        common_columns = sorted(mart_columns & parquet_columns)
        results["columns"] = {
            "mart_only": sorted(mart_columns - parquet_columns),
            "parquet_only": sorted(parquet_columns - mart_columns),
            "common": common_columns,
        }
        if not common_columns:
            failures.append("No common column names between mart and parquet")

        # --- Null rates for key columns ---
        key_in_both = [c for c in KEY_COLUMNS if c in common_columns]
        for col in key_in_both:
            mart_nulls = con.execute(
                f'SELECT count(*) FROM main.mart_tasks_enriched WHERE "{col}" IS NULL'
            ).fetchone()[0]
            null_rates_mart[col] = (mart_nulls / mart_count * 100.0) if mart_count else 0.0
            parquet_nulls = con.execute(
                'SELECT count(*) FROM read_parquet(?) WHERE "{}" IS NULL'.format(col),
                [parquet_path_str],
            ).fetchone()[0]
            null_rates_parquet[col] = (
                (parquet_nulls / parquet_count * 100.0) if parquet_count else 0.0
            )
        results["null_rates"] = {"mart": null_rates_mart, "parquet": null_rates_parquet}

        # --- Sample value comparison for taskstatus, flowname ---
        sample_cols = [c for c in ["taskstatus", "flowname"] if c in common_columns]
        for col in sample_cols:
            mart_counts = dict(
                con.execute(
                    f'SELECT "{col}", count(*) FROM main.mart_tasks_enriched GROUP BY 1'
                ).fetchall()
            )
            parquet_counts = dict(
                con.execute(
                    'SELECT "{}", count(*) FROM read_parquet(?) GROUP BY 1'.format(col),
                    [parquet_path_str],
                ).fetchall()
            )
            value_counts[col] = {"mart": mart_counts, "parquet": parquet_counts}
        results["taskstatus_flowname"] = value_counts
    finally:
        con.close()
        os.chdir(prev_cwd)

    # --- Report ---
    print("=" * 60)
    print("DBT vs PIPELINE COMPARISON")
    print("  DuckDB:  ", DUCKDB_PATH)
    print("  Parquet: ", PARQUET_PATH)
    print("=" * 60)

    print("\n1. ROW COUNTS")
    print(f"   mart_tasks_enriched: {mart_count}")
    print(f"   parquet (source):    {parquet_count}")
    if mart_count == parquet_count:
        print("   -> PASS (equal)")
    else:
        print("   -> FAIL (not equal)")

    print("\n2. COLUMN NAMES (present in both)")
    print(
        f"   Common ({len(common_columns)}): {common_columns[:12]}"
        f"{'...' if len(common_columns) > 12 else ''}"
    )
    if results["columns"]["mart_only"]:
        print(
            f"   Mart only:    {results['columns']['mart_only'][:8]}"
            f"{'...' if len(results['columns']['mart_only']) > 8 else ''}"
        )
    if results["columns"]["parquet_only"]:
        print(
            f"   Parquet only: {results['columns']['parquet_only'][:8]}"
            f"{'...' if len(results['columns']['parquet_only']) > 8 else ''}"
        )
    print("   -> PASS (comparison done)")

    print("\n3. NULL RATES (key columns)")
    for col in key_in_both:
        m, p = null_rates_mart[col], null_rates_parquet[col]
        print(f"   {col}: mart={m:.1f}% null, parquet={p:.1f}% null")
    print("   -> PASS (reported)")

    print("\n4. SAMPLE VALUE COMPARISON (taskstatus, flowname)")
    for col in sample_cols:
        print(f"   {col}:")
        print(f"     mart:    {results['taskstatus_flowname'][col]['mart']}")
        print(f"     parquet: {results['taskstatus_flowname'][col]['parquet']}")
    print("   -> PASS (reported; mart values may be normalized)")

    print("\n" + "=" * 60)
    if failures:
        print("RESULT: FAIL")
        for f in failures:
            print("  -", f)
    else:
        print("RESULT: PASS (row counts match; checks reported above)")
    print("=" * 60)


if __name__ == "__main__":
    main()
