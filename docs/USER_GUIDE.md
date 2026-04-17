# Excel to Power BI Pipeline - User Guide

This tool converts your Excel files into a format Power BI can read quickly.

---

## Dev vs Prod (Environments)

**The default is always dev** so that normal use uses development data and files. You only need to switch to prod when you want to run or connect to production.

### 1. Setting the environment (PIPELINE_ENV)

- **Windows (Command Prompt):**  
  `set PIPELINE_ENV=prod`
- **Windows (PowerShell):**  
  `$env:PIPELINE_ENV="prod"`
- **macOS/Linux:**  
  `export PIPELINE_ENV=prod`

If you do not set `PIPELINE_ENV`, it defaults to **dev**.

### 2. External data folder (`DATA_ROOT`)

By default, all Excel files, Parquet, SQLite, and DuckDB live in **`ExcelIngestion_Data`** next to your project folder (not inside the git repo).

**First time:** run `python scripts/init_data_directory.py` from the project root.  
**Custom location:** set `DATA_ROOT` (PowerShell: `$env:DATA_ROOT="C:\path\to\data"`) before init or pipeline runs.

### 3. The --env flag for run_pipeline.py

With `--dataset`, paths are **`{DATA_ROOT}/{env}/{dataset}/`** (default `DATA_ROOT` = `..\ExcelIngestion_Data`):

- **Dev (default):**  
  `python run_pipeline.py --dataset tasks` → `ExcelIngestion_Data\dev\tasks\`
- **Prod:**  
  `python run_pipeline.py --env prod --dataset tasks` → `ExcelIngestion_Data\prod\tasks\`

`--env` can be `dev` or `prod` (default is `dev`). The pipeline prints the data root and dataset path at start.

### 4. Folder structure (dev vs prod)

| Environment | Where your Excel files go | SQLite DB | Power BI DuckDB file |
|-------------|---------------------------|-----------|------------------------|
| **dev**     | `{DATA_ROOT}\dev\tasks\raw\`, `{DATA_ROOT}\dev\dept_mapping\raw\`, … | `{DATA_ROOT}\analytics\dev_warehouse.db` | `{DATA_ROOT}\powerbi\dev_warehouse.duckdb` |
| **prod**    | `{DATA_ROOT}\prod\...` | `{DATA_ROOT}\analytics\warehouse.db` | `{DATA_ROOT}\powerbi\warehouse.duckdb` |

### 5. Power BI connection strings for each environment

DuckDB files are under **`{DATA_ROOT}\powerbi\`**. Run `python powerbi/setup_odbc.py` to print the full path and connection string for the current `PIPELINE_ENV` (dev or prod).

### 6. Default is always dev

- If you do not set `PIPELINE_ENV` or `--env`, the pipeline and Power BI scripts use **dev**.
- This avoids accidentally writing or reading production data when you meant to use development.

---

## What You Need Before Starting

1. **Python installed** (two versions needed)
   - **Python 3.14** (or 3.10+) for the main pipeline
   - **Python 3.12** for dbt (dbt does not work with Python 3.14)
   - Download from: https://www.python.org/downloads/
   - During install, check "Add Python to PATH"

2. **DuckDB ODBC Driver** (for Power BI connection)
   - Download from: https://duckdb.org/docs/api/odbc/overview
   - Run the installer

---

## First-Time Setup (Do This Once)

### Step 1: Open Command Prompt

Press `Win + R`, type `cmd`, press Enter.

### Step 2: Go to the Project Folder

```
cd "C:\Users\quang\CRC Code\ExcelIngestion"
```

### Step 3: Create a Virtual Environment

```
python -m venv .venv
```

### Step 4: Activate the Virtual Environment

```
.venv\Scripts\activate
```

You should see `(.venv)` at the start of the command line.

### Step 5: Install Required Packages

```
pip install -r requirements.txt
```

Wait for it to finish (may take 1-2 minutes).

### Step 6: Create the external data folder

From the same project folder (with `.venv` activated):

```
python scripts/init_data_directory.py
```

This creates `C:\Users\quang\CRC Code\ExcelIngestion_Data` (next to `ExcelIngestion`) and copies YAML configs from the repo. Put your Excel files under `ExcelIngestion_Data\dev\...\raw\`, not under `datasets\` in the repo, unless you are using the migration script.

### Step 7: Set Up dbt (Separate Python Environment)

dbt requires Python 3.10-3.12. Since the main `.venv` uses Python 3.14, we need a separate virtual environment:

```
py -3.12 -m venv .venv-dbt
.venv-dbt\Scripts\activate
pip install dbt-core dbt-duckdb
```

Verify dbt works (set `DATA_ROOT` to your external folder if it is not the default):

```
dbt --version
cd dbt_crc
set DATA_ROOT=C:\Users\quang\CRC Code\ExcelIngestion_Data
dbt debug
```

You should see "All checks passed!"

---

## Adding Your Excel Files

**Run the init script first** (once per machine or after a fresh clone): `python scripts/init_data_directory.py`. That creates `ExcelIngestion_Data` next to the project and seeds config YAML. Without it, `run_pipeline.py` will not find `pipeline.yaml` under the external tree.

Default environment is **dev**. Put files under **`ExcelIngestion_Data`** (or your custom **`DATA_ROOT`**), not under `datasets\` in the repo:

1. Open File Explorer
2. Go to: `C:\Users\quang\CRC Code\ExcelIngestion_Data\dev\tasks\raw`
3. Copy your Excel files (`.xlsx`) into this folder

For employee/department data:

- `ExcelIngestion_Data\dev\dept_mapping\raw`

For unified employee dimension (HR + Genpact):

- `ExcelIngestion_Data\dev\employees_master\raw`

For Workday worker data:

- `ExcelIngestion_Data\dev\workers\raw`

For revenue data:

- `ExcelIngestion_Data\dev\revenue\raw`

For employee onboarding/launch tracking:

- `ExcelIngestion_Data\dev\launch\raw`

For production, use `ExcelIngestion_Data\prod\...` and run the pipeline with `--env prod`.

**If you still have files under the old in-repo `datasets\` tree:** run `python scripts/migrate_data.py --dry-run` then `python scripts/migrate_data.py` to move them into `ExcelIngestion_Data`.

---

## Running the Pipeline

### Step 1: Open Command Prompt and Activate

```
cd "C:\Users\quang\CRC Code\ExcelIngestion"
.venv\Scripts\activate
```

### Step 2: Run the Pipeline

Default is **dev**. For task data:
```
python run_pipeline.py
```
Or explicitly: `python run_pipeline.py --dataset tasks`

For employee data:
```
python run_pipeline.py --dataset dept_mapping
```

For unified employee dimension:
```
python run_pipeline.py --dataset employees_master
```

For workers (Workday HR export):
```
python run_pipeline.py --dataset workers
```

For revenue data:
```
python run_pipeline.py --dataset revenue
```

For launch/onboarding tracking:
```
python run_pipeline.py --dataset launch
```

For production:
```
python run_pipeline.py --env prod --dataset tasks
python run_pipeline.py --env prod --dataset dept_mapping
```

Wait for it to finish. You'll see the environment (e.g. "Environment: dev") and progress messages.

### Step 3: Run dbt to Build Analytics Models

After the Python pipeline completes, run dbt to build the analytics marts. Use the same **`DATA_ROOT`** as the pipeline (default: `ExcelIngestion_Data` next to the project):

```
.venv-dbt\Scripts\activate
set DATA_ROOT=C:\Users\quang\CRC Code\ExcelIngestion_Data
cd dbt_crc
dbt seed
dbt run
dbt test
```

On a new DuckDB file, **`dbt seed`** is required before **`dbt run`**. You should see "Completed successfully" when models pass (use `dbt run --select ...` if you have not ingested every dataset yet).

### Step 4: Verify in Power BI

The DuckDB file at `ExcelIngestion_Data\powerbi\dev_warehouse.duckdb` (default `DATA_ROOT`) contains:
- Base tables: `tasks`, `employees`, `employees_master`, `workers`, `revenue`, `launch`
- Staging views: `stg_tasks`, `stg_workers`, etc.
- Mart tables: `mart_tasks_enriched`, `mart_team_capacity`, etc.

---

## Connecting Power BI

### Step 1: Open Power BI Desktop

### Step 2: Get Data

1. Click **Get Data** (Home tab)
2. Click **More...**
3. Search for **ODBC**
4. Click **ODBC**, then **Connect**

### Step 3: Enter Connection String

1. Click **Advanced options**
2. In the **Connection string** box, paste the string for your environment (default is **dev**):

   **Dev:**  
   `Driver={DuckDB Driver};Database=C:\Users\quang\CRC Code\ExcelIngestion_Data\powerbi\dev_warehouse.duckdb;access_mode=READ_ONLY`

   **Prod:**  
   `Driver={DuckDB Driver};Database=C:\Users\quang\CRC Code\ExcelIngestion_Data\powerbi\warehouse.duckdb;access_mode=READ_ONLY`

   Replace with your `DATA_ROOT` if needed. Run `python powerbi/setup_odbc.py` to print the exact path and string.

3. Click **OK**

### Step 4: Select Your Tables

You'll see these tables:
- `tasks` - All your task data
- `employees` - Employee/department info (dept_mapping)
- `employees_master` - Unified employee dimension (HR + Genpact)
- `workers` - Workday HR worker data
- `revenue` - Revenue by broker/team/division
- `launch` - Employee onboarding tracking
- `tasks_with_dept` - Tasks joined with employee info (recommended)

And these pre-calculated analytics marts:
- `mart_tasks_enriched` - Tasks with employee info and duration metrics
- `mart_team_capacity` - Headcount by department
- `mart_team_demand` - Task volume by department and date
- `mart_backlog` - Open tasks with age
- `mart_turnaround` - Completed task performance
- `mart_daily_trend` - Daily opened vs completed

Select the tables you need, click **Load**.

---

## Updating Your Data

When you have new Excel files:

1. Delete old files from the `raw` folder for your environment (e.g. `ExcelIngestion_Data\dev\tasks\raw`) or add new ones alongside.
2. Run the pipeline again (default is dev):
   ```
   cd "C:\Users\quang\CRC Code\ExcelIngestion"
   .venv\Scripts\activate
   python run_pipeline.py
   python run_pipeline.py --dataset dept_mapping
   python powerbi/create_duckdb.py
   ```
   For prod, use `--env prod` and `set PIPELINE_ENV=prod` before create_duckdb.py.
3. In Power BI, click **Refresh**

---

## Schema Comparison Tool

When new Excel files arrive with different columns, the pipeline may fail at the combine step. Use the schema comparison tool to check for column differences **before** running the pipeline.

### Standalone Usage

Compare all Excel files in a dataset's raw/ folder:
```
python scripts/compare_schemas.py --dataset tasks --env dev
```

Compare against the expected schema.yaml (shows aliases and missing/extra columns):
```
python scripts/compare_schemas.py --dataset tasks --env dev --check-against
```

Compare all files against a specific baseline file:
```
python scripts/compare_schemas.py --dataset tasks --env dev --baseline "Jan 2025.xlsx"
```

Export full report as JSON:
```
python scripts/compare_schemas.py --dataset tasks --env dev --output schema_report.json
```

### Preflight Check (Recommended)

Run schema validation before the pipeline with `--preflight`:
```
python run_pipeline.py --dataset tasks --preflight
python run_pipeline.py --all --preflight --dry-run
```

**Preflight behavior:**
- **Warns** if source files have extra columns (they will be dropped by step 02)
- **Fails** if schema.yaml expects columns missing from ALL source files (likely a config error)
- **Passes** and continues if everything looks good

This catches schema drift early, before the pipeline spends time processing data that will fail later.

---

## One-Command Refresh (Pipeline + dbt)

Instead of running the pipeline and dbt separately, use `refresh.py` to do both in one command:

```
python refresh.py --dataset tasks
```

This runs the Python pipeline (steps 1-9) and then runs dbt to rebuild analytics marts.

### Common refresh.py Commands

| Task | Command |
|------|---------|
| Refresh single dataset (dev) | `python refresh.py --dataset tasks` |
| Refresh all datasets (dev) | `python refresh.py --all` |
| Refresh single dataset (prod) | `python refresh.py --env prod --dataset tasks` |
| Skip pipeline, run dbt only | `python refresh.py --skip-pipeline --dataset tasks` |
| Skip dbt, run pipeline only | `python refresh.py --skip-dbt --dataset tasks` |
| Force reprocess all files | `python refresh.py --dataset tasks --force` |

**Note**: You do NOT need to activate the dbt venv first—`refresh.py` automatically uses `.venv-dbt\Scripts\dbt.exe`.

---

## Quick Reference

| Task | Command |
|------|---------|
| Activate pipeline venv | `.venv\Scripts\activate` |
| Activate dbt venv | `.venv-dbt\Scripts\activate` |
| Run task pipeline (dev) | `python run_pipeline.py` or `python run_pipeline.py --dataset tasks` |
| Run task pipeline (prod) | `python run_pipeline.py --env prod --dataset tasks` |
| Run employee pipeline | `python run_pipeline.py --dataset dept_mapping` |
| Run employees_master | `python run_pipeline.py --dataset employees_master` |
| Run workers pipeline | `python run_pipeline.py --dataset workers` |
| Run revenue pipeline | `python run_pipeline.py --dataset revenue` |
| Run launch pipeline | `python run_pipeline.py --dataset launch` |
| Run all datasets | `python run_pipeline.py --all` |
| **One-command refresh** | `python refresh.py --dataset tasks` |
| **Refresh all + dbt** | `python refresh.py --all` |
| Run with preflight check | `python run_pipeline.py --dataset tasks --preflight` |
| Compare schemas | `python scripts/compare_schemas.py --dataset tasks --env dev` |
| Check against schema.yaml | `python scripts/compare_schemas.py --dataset tasks --env dev --check-against` |
| Initialize external data folder | `python scripts/init_data_directory.py` |
| Run dbt models | `.venv-dbt\Scripts\activate` then `set DATA_ROOT=...` and `cd dbt_crc && dbt seed && dbt run` |
| Test dbt models | `dbt test` |
| Test ODBC / print connection string | `python powerbi/setup_odbc.py` |

### Two Virtual Environments

| venv | Python | Purpose |
|------|--------|---------|
| `.venv` | 3.14 | Main pipeline (`run_pipeline.py`) |
| `.venv-dbt` | 3.12 | dbt models (`dbt run`) |

**Important**: Always activate the correct venv before running commands!

---

## Troubleshooting

### "python is not recognized"
- Reinstall Python and check "Add Python to PATH"
- Or use full path: `C:\Python310\python.exe`

### "No module named pandas"
- Make sure you activated the environment: `.venv\Scripts\activate`
- Reinstall packages: `pip install -r requirements.txt`

### Power BI shows no tables
- Make sure you ran `python powerbi/create_duckdb.py`
- Check the connection string has the correct path
- Try running `python powerbi/setup_odbc.py` to test

### "DuckDB Driver not found"
- Install the DuckDB ODBC driver from https://duckdb.org/docs/api/odbc/overview
- Restart your computer after installing

### Pipeline fails with error
- Check your Excel files are under `ExcelIngestion_Data\{env}\{dataset}\raw\` (or your `DATA_ROOT`)
- Run `python scripts/init_data_directory.py` if you see `Pipeline config not found`
- Make sure Excel files are not open in another program
- Try running with `--dry-run` first: `python run_pipeline.py --dry-run`

---

## File Locations

Default environment is **dev**. Paths assume default **`DATA_ROOT`** = `ExcelIngestion_Data` next to the project.

| What | Dev | Prod |
|------|-----|------|
| Excel files (tasks) | `ExcelIngestion_Data\dev\tasks\raw\` | `ExcelIngestion_Data\prod\tasks\raw\` |
| Excel files (employees) | `ExcelIngestion_Data\dev\dept_mapping\raw\` | `ExcelIngestion_Data\prod\dept_mapping\raw\` |
| Excel files (employees_master) | `ExcelIngestion_Data\dev\employees_master\raw\` | `ExcelIngestion_Data\prod\employees_master\raw\` |
| Excel files (workers) | `ExcelIngestion_Data\dev\workers\raw\` | `ExcelIngestion_Data\prod\workers\raw\` |
| Excel files (revenue) | `ExcelIngestion_Data\dev\revenue\raw\` | `ExcelIngestion_Data\prod\revenue\raw\` |
| Excel files (launch) | `ExcelIngestion_Data\dev\launch\raw\` | `ExcelIngestion_Data\prod\launch\raw\` |
| Combined Parquet | `ExcelIngestion_Data\dev\{dataset}\analytics\combined.parquet` | `ExcelIngestion_Data\prod\{dataset}\analytics\combined.parquet` |
| SQLite warehouse | `ExcelIngestion_Data\analytics\dev_warehouse.db` | `ExcelIngestion_Data\analytics\warehouse.db` |
| Power BI DuckDB | `ExcelIngestion_Data\powerbi\dev_warehouse.duckdb` | `ExcelIngestion_Data\powerbi\warehouse.duckdb` |
| Pipeline logs | `ExcelIngestion_Data\dev\tasks\logs\pipeline.log` | `ExcelIngestion_Data\prod\tasks\logs\pipeline.log` |

---

## Getting Help

If you run into issues:
1. Check the Troubleshooting section above
2. Look at the log file: `ExcelIngestion_Data\dev\tasks\logs\pipeline.log` (adjust env/dataset as needed)
3. Contact your system administrator
