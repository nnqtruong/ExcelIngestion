"""
Power BI ODBC setup: print warehouse path and test DuckDB ODBC connection.

Use the printed absolute path in your Windows ODBC DSN (Database parameter)
so Power BI connects to the file warehouse, not an in-memory database.
"""
from pathlib import Path

# Resolve absolute path to warehouse.duckdb (same folder as this script)
db_path = (Path(__file__).resolve().parent / "warehouse.duckdb").resolve()

print("=" * 60)
print("DuckDB warehouse path (use this in ODBC DSN)")
print("=" * 60)
print(f"\nFull absolute path:\n  {db_path}\n")
print(f"File exists: {db_path.exists()}")
if db_path.exists():
    print(f"File size: {db_path.stat().st_size / 1024 / 1024:.1f} MB")
print()

# Test ODBC connection with explicit path (no DSN)
try:
    import pyodbc

    conn_str = f"Driver={{DuckDB Driver}};Database={db_path};access_mode=READ_ONLY"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # List all tables
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print(f"Tables found: {[t[0] for t in tables]}")

    # Row counts
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
        count = cursor.fetchone()[0]
        print(f"  {table[0]}: {count} rows")

    conn.close()
    print("\nODBC connection successful.")
except ImportError:
    print("pyodbc not installed. Run: pip install pyodbc")
    raise
except Exception as e:
    print(f"ODBC connection failed: {e}")
    raise

# Instructions for Windows ODBC DSN
print("\n" + "=" * 60)
print("Update Windows ODBC DSN for Power BI")
print("=" * 60)
print("""
1. Open ODBC Data Source Administrator (64-bit)
   - Win + R, type odbcad32, Enter (64-bit; use SysWOW64\\odbcad32.exe for 32-bit)

2. User DSN or System DSN tab: Select "DuckDB", then Configure

3. Set:
   - Database: paste the full absolute path printed above
   - access_mode: READ_ONLY

4. OK, then Test connection.

If the DSN configuration dialog has no "Database" field:
  Use a DSN-less connection in Power BI:
  - Get Data, Other, ODBC, Connect
  - Advanced options: paste this connection string (replace <PATH> with the path above):

  Driver={DuckDB Driver};Database=<PATH>;access_mode=READ_ONLY

  Example:
  Driver={DuckDB Driver};Database=""" + str(db_path) + """;access_mode=READ_ONLY
""")
print("=" * 60)
