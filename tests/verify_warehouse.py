"""One-off verification of analytics/warehouse.db after full pipeline run."""
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "analytics" / "warehouse.db"

def main():
    if not DB.exists():
        print(f"FAIL: {DB} not found")
        return 1
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # Tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    print("Tables:", tables)
    if "tasks" not in tables or "employees" not in tables:
        print("FAIL: expected both 'tasks' and 'employees' tables")
        conn.close()
        return 1

    queries = [
        ("SELECT COUNT(*) FROM tasks", "tasks row count"),
        ("SELECT COUNT(*) FROM employees", "employees row count"),
        ("SELECT COUNT(DISTINCT division) FROM employees", "distinct divisions in employees"),
        ("SELECT * FROM v_tasks_by_department WHERE full_name IS NOT NULL LIMIT 10", "v_tasks_by_department sample (full_name NOT NULL)"),
        ("SELECT division, COUNT(*) as task_count FROM v_tasks_by_department GROUP BY division", "task_count by division"),
    ]
    for sql, label in queries:
        print(f"\n--- {label} ---")
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else None
        if cols:
            print("Columns:", cols)
        for r in rows:
            print(r)
        print(f"Rows: {len(rows)}")
    conn.close()
    print("\nVerification done.")
    return 0

if __name__ == "__main__":
    exit(main())
