# Power BI + DuckDB

This folder contains the DuckDB warehouse and scripts used to feed **Power BI Desktop**. The database is built from the Excel ingestion pipeline; report tables are flat and pre-joined so you can import them without defining relationships.

---

## Prerequisites

- **DuckDB ODBC driver** — [Download and install](https://duckdb.org/docs/api/odbc/overview) the driver for your OS (64-bit on Windows).
- **Power BI Desktop** — [Download](https://powerbi.microsoft.com/desktop) if needed.
- **Pipeline run at least once** — From the project root:
  ```bash
  python powerbi/refresh.py
  ```
  This creates `powerbi/warehouse.duckdb` and all report tables.

---

## Option A: ODBC Connection (recommended)

Use the DuckDB ODBC driver so Power BI can read the database directly. Plan for about 10 minutes.

### Step 1: Install the DuckDB ODBC driver

- Go to [DuckDB ODBC overview](https://duckdb.org/docs/api/odbc/overview).
- Download and install the **64-bit** Windows driver if you use 64-bit Power BI Desktop.

### Step 2: Create a User DSN

1. Open **ODBC Data Source Administrator (64-bit)**  
   (Windows: search “ODBC” or run `C:\Windows\System32\odbcad32.exe`).
2. Go to the **User DSN** tab → **Add**.
3. Select **DuckDB Driver** → Finish.
4. Configure:
   - **Database path:** Full path to the DuckDB file, e.g.  
     `C:\Users\YourName\Code\Reporting\ExcelIngestion\powerbi\warehouse.duckdb`
   - **Data Source Name:** `ExcelIngestion_DuckDB`
5. Click **OK** to save the DSN.

### Step 3: Connect from Power BI Desktop

1. **Get Data** → **Other** → **ODBC** → Connect.
2. **Data Source name (DSN):** choose **ExcelIngestion_DuckDB** → OK.
3. In **Navigator**, select the **report_*** tables (these are the ones intended for reporting):
   - `report_tasks_full`
   - `report_tasks_by_originator`
   - `report_daily_volume`
   - `report_drawer_performance`
   - `report_carrier_workload`
   - `report_team_workload`
4. Check the tables you need → **Load**.
5. Use **Import** mode (not DirectQuery) for best compatibility.

You can now build reports on the loaded report tables; no relationships are required between them unless you choose to add some.

---

## Option B: Python Script Data Source

If you prefer (or must) use Power BI’s **Get Data → Python script**, you can load a single report table per script. Power BI runs the script and uses the resulting pandas DataFrame.

1. **Get Data** → **Other** → **Python script** → Connect.
2. Paste one of the scripts below, then replace `C:\path\to\ExcelIngestion\powerbi\warehouse.duckdb` with the **full path** to your `powerbi/warehouse.duckdb` file.
3. Run the script; Power BI will show the table. Click **OK** and then **Load**.

**Script for `report_tasks_full`:**
```python
import duckdb
import pandas as pd
conn = duckdb.connect(r'C:\path\to\ExcelIngestion\powerbi\warehouse.duckdb', read_only=True)
dataset = conn.execute("SELECT * FROM report_tasks_full").fetchdf()
conn.close()
```

**Script for `report_tasks_by_originator`:**
```python
import duckdb
import pandas as pd
conn = duckdb.connect(r'C:\path\to\ExcelIngestion\powerbi\warehouse.duckdb', read_only=True)
dataset = conn.execute("SELECT * FROM report_tasks_by_originator").fetchdf()
conn.close()
```

**Script for `report_daily_volume`:**
```python
import duckdb
import pandas as pd
conn = duckdb.connect(r'C:\path\to\ExcelIngestion\powerbi\warehouse.duckdb', read_only=True)
dataset = conn.execute("SELECT * FROM report_daily_volume").fetchdf()
conn.close()
```

**Script for `report_drawer_performance`:**
```python
import duckdb
import pandas as pd
conn = duckdb.connect(r'C:\path\to\ExcelIngestion\powerbi\warehouse.duckdb', read_only=True)
dataset = conn.execute("SELECT * FROM report_drawer_performance").fetchdf()
conn.close()
```

**Script for `report_carrier_workload`:**
```python
import duckdb
import pandas as pd
conn = duckdb.connect(r'C:\path\to\ExcelIngestion\powerbi\warehouse.duckdb', read_only=True)
dataset = conn.execute("SELECT * FROM report_carrier_workload").fetchdf()
conn.close()
```

**Script for `report_team_workload`:**
```python
import duckdb
import pandas as pd
conn = duckdb.connect(r'C:\path\to\ExcelIngestion\powerbi\warehouse.duckdb', read_only=True)
dataset = conn.execute("SELECT * FROM report_team_workload").fetchdf()
conn.close()
```

Use one script per report table; repeat **Get Data → Python script** for each table you want in the report.

---

## Available Report Tables

| Table | Description |
|-------|-------------|
| **report_tasks_full** | Tasks with assignee info (full_name, employee_title, division, team, divisionid) and computed duration_minutes, duration_hours, lifecycle_hours, task_date. |
| **report_tasks_by_originator** | Same task and duration fields, joined by originator (taskfrom): from_name, from_title, from_division, from_team. |
| **report_daily_volume** | One row per task_date: tasks_initiated, tasks_completed, in_progress. |
| **report_drawer_performance** | One row per drawer: total_tasks, completed, avg_duration_hours. |
| **report_carrier_workload** | One row per carrier + flowname: task_count, completed, in_progress, pending. |
| **report_team_workload** | One row per team + division (from employees): total_tasks, completed, avg_duration_hours. |

**Columns (summary):**

- **report_tasks_full:** row_id, taskid, drawer, policynumber, filename, filenumber, effectivedate, carrier, acctexec, taskdescription, assignedto, taskfrom, operationby, flowname, stepname, sentto, dateavailable, dateinitiated, dateended, taskstatus, starttime, endtime, full_name, employee_title, division, team, divisionid, duration_minutes, duration_hours, lifecycle_hours, task_date
- **report_tasks_by_originator:** Same task columns plus from_name, from_title, from_division, from_team and the same duration/task_date fields.
- **report_daily_volume:** task_date, tasks_initiated, tasks_completed, in_progress
- **report_drawer_performance:** drawer, total_tasks, completed, avg_duration_hours
- **report_carrier_workload:** carrier, flowname, task_count, completed, in_progress, pending
- **report_team_workload:** team, division, total_tasks, completed, avg_duration_hours

---

## Refreshing Data

1. **Rebuild the DuckDB file** from the project root:
   ```bash
   python powerbi/refresh.py
   ```
   This reruns the tasks and dept_mapping pipelines, then rebuilds the DuckDB views and report tables.

2. In **Power BI Desktop**, click **Refresh** so the report uses the updated data.

**Scheduled refresh (Power BI Service):** To refresh a report that uses this DuckDB file on a schedule, the file must be reachable via an **on-premises data gateway** (e.g. the machine that runs `refresh.py` and holds `powerbi/warehouse.duckdb`). Point the gateway data source to the same path used in your DSN or Python script.

---

## Troubleshooting

| Issue | What to do |
|-------|------------|
| **ODBC driver not found** | Install the 64-bit DuckDB ODBC driver and use “ODBC Data Source Administrator (64-bit)”. Restart Power BI after installing. |
| **Wrong path / file not found** | In the DSN (or Python script), use the **full path** to `powerbi/warehouse.duckdb`. Run `python powerbi/connect_test.py` to confirm the file exists and path. |
| **Database is locked** | Close any other app using the file (Python, another Power BI session, DuckDB CLI). Then run `refresh.py` or open the file again. |
| **DuckDB version mismatch** | Keep the DuckDB ODBC driver and the Python `duckdb` package in sync when you upgrade (e.g. same major/minor version) to avoid compatibility errors. |

For a quick check that the database and report tables are valid, run from the project root:

```bash
python powerbi/connect_test.py
```

This prints table/view list, sample row counts, and exports a sample query to `powerbi/sample_output.csv`.
