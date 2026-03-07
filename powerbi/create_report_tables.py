"""
Materialize flat report tables in powerbi/warehouse.duckdb for Power BI.
Opens existing database (create_views.py must have run first). Drops and recreates
report tables each run.
"""
from pathlib import Path

import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
DUCKDB_PATH = SCRIPT_DIR / "warehouse.duckdb"


def main() -> None:
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {DUCKDB_PATH}. Run powerbi/create_views.py first."
        )

    conn = duckdb.connect(str(DUCKDB_PATH))

    # report_tasks_full — tasks LEFT JOIN employees on assignedto, all task cols + employee cols + computed
    conn.execute("DROP TABLE IF EXISTS report_tasks_full")
    conn.execute("""
        CREATE TABLE report_tasks_full AS
        SELECT
            t.*,
            e.full_name,
            e.title AS employee_title,
            e.division,
            e.team,
            e.divisionid,
            ROUND((epoch(t.endtime) - epoch(t.starttime)) / 60.0, 2) AS duration_minutes,
            ROUND((epoch(t.endtime) - epoch(t.starttime)) / 3600.0, 2) AS duration_hours,
            ROUND((epoch(t.dateended) - epoch(t.dateinitiated)) / 3600.0, 2) AS lifecycle_hours,
            CAST(t.dateinitiated AS DATE) AS task_date
        FROM tasks t
        LEFT JOIN employees e ON LOWER(t.assignedto) = LOWER(e.userid)
    """)

    # report_tasks_by_originator — JOIN on taskfrom, employee fields as from_*
    conn.execute("DROP TABLE IF EXISTS report_tasks_by_originator")
    conn.execute("""
        CREATE TABLE report_tasks_by_originator AS
        SELECT
            t.*,
            e.full_name AS from_name,
            e.title AS from_title,
            e.division AS from_division,
            e.team AS from_team,
            ROUND((epoch(t.endtime) - epoch(t.starttime)) / 60.0, 2) AS duration_minutes,
            ROUND((epoch(t.endtime) - epoch(t.starttime)) / 3600.0, 2) AS duration_hours,
            ROUND((epoch(t.dateended) - epoch(t.dateinitiated)) / 3600.0, 2) AS lifecycle_hours,
            CAST(t.dateinitiated AS DATE) AS task_date
        FROM tasks t
        LEFT JOIN employees e ON LOWER(t.taskfrom) = LOWER(e.userid)
    """)

    # report_daily_volume — materialized v_daily_volume
    conn.execute("DROP TABLE IF EXISTS report_daily_volume")
    conn.execute("CREATE TABLE report_daily_volume AS SELECT * FROM v_daily_volume")

    # report_drawer_performance — materialized v_drawer_summary
    conn.execute("DROP TABLE IF EXISTS report_drawer_performance")
    conn.execute("CREATE TABLE report_drawer_performance AS SELECT * FROM v_drawer_summary")

    # report_carrier_workload — materialized v_carrier_workload
    conn.execute("DROP TABLE IF EXISTS report_carrier_workload")
    conn.execute("CREATE TABLE report_carrier_workload AS SELECT * FROM v_carrier_workload")

    # report_team_workload — materialized v_team_workload
    conn.execute("DROP TABLE IF EXISTS report_team_workload")
    conn.execute("CREATE TABLE report_team_workload AS SELECT * FROM v_team_workload")

    # Print each report table: name, row count, column count
    report_tables = [
        "report_tasks_full",
        "report_tasks_by_originator",
        "report_daily_volume",
        "report_drawer_performance",
        "report_carrier_workload",
        "report_team_workload",
    ]
    print("Report tables (flat for Power BI):")
    for name in report_tables:
        rows = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        cols = conn.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = ?", [name]).fetchone()[0]
        print(f"  {name}: {rows} rows, {cols} columns")

    conn.close()
    print(f"\nDone. Database: {DUCKDB_PATH}")


if __name__ == "__main__":
    main()
