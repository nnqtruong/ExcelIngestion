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

### 2. The --env flag for run_pipeline.py

When you run the pipeline with a dataset name, it uses **datasets/{env}/{dataset}/**:

- **Dev (default):**  
  `python run_pipeline.py --dataset tasks` → uses `datasets/dev/tasks/`
- **Prod:**  
  `python run_pipeline.py --env prod --dataset tasks` → uses `datasets/prod/tasks/`

`--env` can be `dev` or `prod` (default is `dev`). The pipeline prints the current environment at start.

### 3. Folder structure (dev vs prod)

| Environment | Where your data lives | SQLite DB | Power BI DuckDB file |
|-------------|------------------------|-----------|------------------------|
| **dev**     | `datasets\dev\tasks\raw\`, `datasets\dev\dept_mapping\raw\` | `analytics\dev_warehouse.db` | `powerbi\dev_warehouse.duckdb` |
| **prod**    | `datasets\prod\tasks\raw\`, `datasets\prod\dept_mapping\raw\` | `analytics\warehouse.db` | `powerbi\warehouse.duckdb` |

Put Excel files in the folder for the environment you are using (e.g. dev: `datasets\dev\tasks\raw\`).

### 4. Power BI connection strings for each environment

Use the connection string that matches the environment:

- **Dev (default):**  
  `Driver={DuckDB Driver};Database=C:\Users\quang\CRC Code\ExcelIngestion\powerbi\dev_warehouse.duckdb;access_mode=READ_ONLY`
- **Prod:**  
  `Driver={DuckDB Driver};Database=C:\Users\quang\CRC Code\ExcelIngestion\powerbi\warehouse.duckdb;access_mode=READ_ONLY`

Replace the path with your actual project folder. To see the exact string for the current environment, run:  
`python powerbi/setup_odbc.py` (for dev) or `set PIPELINE_ENV=prod` then `python powerbi/setup_odbc.py` (for prod).

### 5. Default is always dev

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

### Step 6: Set Up dbt (Separate Python Environment)

dbt requires Python 3.10-3.12. Since the main `.venv` uses Python 3.14, we need a separate virtual environment:

```
py -3.12 -m venv .venv-dbt
.venv-dbt\Scripts\activate
pip install dbt-core dbt-duckdb
```

Verify dbt works:
```
dbt --version
cd dbt_crc
dbt debug
```

You should see "All checks passed!"

---

## Adding Your Excel Files

Default environment is **dev**. Use these folders:

1. Open File Explorer
2. Go to: `C:\Users\quang\CRC Code\ExcelIngestion\datasets\dev\tasks\raw`
3. Copy your Excel files (`.xlsx`) into this folder

For employee/department data:
- Go to: `C:\Users\quang\CRC Code\ExcelIngestion\datasets\dev\dept_mapping\raw`
- Copy your employee mapping Excel file here

For unified employee dimension (HR + Genpact):
- Go to: `C:\Users\quang\CRC Code\ExcelIngestion\datasets\dev\employees_master\raw`
- Copy Brokerage.xlsx, Select.xlsx, and Genpact.xlsx here

For production, use `datasets\prod\tasks\raw`, `datasets\prod\dept_mapping\raw`, and `datasets\prod\employees_master\raw` and run the pipeline with `--env prod`.

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

For production:
```
python run_pipeline.py --env prod --dataset tasks
python run_pipeline.py --env prod --dataset dept_mapping
```

Wait for it to finish. You'll see the environment (e.g. "Environment: dev") and progress messages.

### Step 3: Run dbt to Build Analytics Models

After the Python pipeline completes, run dbt to build the analytics marts:

```
.venv-dbt\Scripts\activate
cd dbt_crc
dbt run
dbt test
```

You should see "Completed successfully" with all models passing.

### Step 4: Verify in Power BI

The DuckDB file at `powerbi\dev_warehouse.duckdb` now contains:
- Base tables: `tasks`, `employees`, `employees_master`, `workers`, `revenue`
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
   `Driver={DuckDB Driver};Database=C:\Users\quang\CRC Code\ExcelIngestion\powerbi\dev_warehouse.duckdb;access_mode=READ_ONLY`

   **Prod:**  
   `Driver={DuckDB Driver};Database=C:\Users\quang\CRC Code\ExcelIngestion\powerbi\warehouse.duckdb;access_mode=READ_ONLY`

   Replace the path with your project folder. Run `python powerbi/setup_odbc.py` to print the exact string.

3. Click **OK**

### Step 4: Select Your Tables

You'll see these tables:
- `tasks` - All your task data
- `employees` - Employee/department info (dept_mapping)
- `employees_master` - Unified employee dimension (HR + Genpact)
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

1. Delete old files from the `raw` folder for your environment (e.g. `datasets\dev\tasks\raw`) or add new ones alongside.
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

## Quick Reference

| Task | Command |
|------|---------|
| Activate pipeline venv | `.venv\Scripts\activate` |
| Activate dbt venv | `.venv-dbt\Scripts\activate` |
| Run task pipeline (dev) | `python run_pipeline.py` or `python run_pipeline.py --dataset tasks` |
| Run task pipeline (prod) | `python run_pipeline.py --env prod --dataset tasks` |
| Run employee pipeline | `python run_pipeline.py --dataset dept_mapping` |
| Run employees_master | `python run_pipeline.py --dataset employees_master` |
| Run revenue pipeline | `python run_pipeline.py --dataset revenue` |
| Run dbt models | `.venv-dbt\Scripts\activate` then `cd dbt_crc && dbt run` |
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
- Check your Excel files are in the correct `raw` folder
- Make sure Excel files are not open in another program
- Try running with `--dry-run` first: `python run_pipeline.py --dry-run`

---

## File Locations

Default environment is **dev**.

| What | Dev | Prod |
|------|-----|------|
| Excel files (tasks) | `datasets\dev\tasks\raw\` | `datasets\prod\tasks\raw\` |
| Excel files (employees) | `datasets\dev\dept_mapping\raw\` | `datasets\prod\dept_mapping\raw\` |
| Excel files (employees_master) | `datasets\dev\employees_master\raw\` | `datasets\prod\employees_master\raw\` |
| Power BI DuckDB | `powerbi\dev_warehouse.duckdb` | `powerbi\warehouse.duckdb` |
| Pipeline logs | `datasets\dev\tasks\logs\pipeline.log` | `datasets\prod\tasks\logs\pipeline.log` |

---

## Getting Help

If you run into issues:
1. Check the Troubleshooting section above
2. Look at the log file: `datasets\tasks\logs\pipeline.log`
3. Contact your system administrator
