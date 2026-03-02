"""Quick script to query analytics/tasks.db when sqlite3 CLI is not installed."""
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "analytics" / "tasks.db"

if not DB.exists():
    print(f"Database not found: {DB}")
    sys.exit(1)

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# List tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
print("Tables:", ", ".join(tables))

for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM [{t}]")
    print(f"  {t}: {cur.fetchone()[0]} rows")

# If user passed a query, run it
if len(sys.argv) > 1:
    query = " ".join(sys.argv[1:])
    try:
        cur.execute(query)
        rows = cur.fetchall()
        if rows:
            print("\nResult:", rows[0].keys() if hasattr(rows[0], "keys") else "columns")
            for r in rows[:20]:
                print(dict(r) if hasattr(r, "keys") else r)
            if len(rows) > 20:
                print(f"... and {len(rows) - 20} more rows")
        else:
            print("(no rows)")
    except sqlite3.Error as e:
        print("Error:", e)
else:
    print("\nUsage: python scripts/query_db.py \"SELECT * FROM tasks LIMIT 5\"")

conn.close()
