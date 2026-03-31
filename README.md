# Excel Ingestion Pipeline

A 10-step data pipeline that converts Excel files into clean, validated Parquet datasets and exports to SQLite/DuckDB for Power BI. Handles 500K+ row Excel files with low memory usage.

## First-Time Setup

### 1. Clone and Create Virtual Environment

```bash
cd "C:\Users\quang\CRC Code"
git clone <repo-url> ExcelIngestion
cd ExcelIngestion

# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (macOS/Linux)
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

Required packages: `pandas`, `pyarrow`, `openpyxl`, `pyyaml`, `pytest`, `duckdb`, `psutil`, `pyodbc`

### 3. Add Your Excel Files

Place Excel files in the dataset's `raw/` folder for the environment you use (default: **dev**):

```
datasets/dev/tasks/raw/              # Task data (*.xlsx) — dev
datasets/dev/dept_mapping/raw/       # Employee/department mapping (*.xlsx) — dev
datasets/dev/employees_master/raw/   # Unified employee dimension (*.xlsx) — dev
datasets/prod/tasks/raw/             # Production task data (when using --env prod)
```

### 4. Run the Pipeline

```bash
# Run tasks dataset (dev, default)
python run_pipeline.py
python run_pipeline.py --dataset tasks

# Run dept_mapping (dev)
python run_pipeline.py --dataset dept_mapping

# Run employees_master (unified HR dimension)
python run_pipeline.py --dataset employees_master

# Production
python run_pipeline.py --env prod --dataset tasks
python run_pipeline.py --env prod --dataset dept_mapping

# Dry run (validate configs only)
python run_pipeline.py --dry-run
```

### 5. Connect Power BI via DuckDB ODBC

```bash
# Create DuckDB database for Power BI (uses dev by default)
python powerbi/create_duckdb.py

# Prod: set env then create
set PIPELINE_ENV=prod
python powerbi/create_duckdb.py

# Test ODBC connection (prints connection string for current env)
python powerbi/setup_odbc.py
```

In Power BI, use the connection string for your environment (see [Dev/Prod Environments](#devprod-environments)). Default is **dev** (`powerbi/dev_warehouse.duckdb`).

## dbt Setup (Separate Python Environment)

dbt requires Python 3.10-3.12. Since the main `.venv` uses Python 3.14, a separate venv is required:

### Create dbt venv (one-time)

```bash
py -3.12 -m venv .venv-dbt
.venv-dbt\Scripts\activate
pip install dbt-core dbt-duckdb
```

### Running dbt

```bash
# Always activate the dbt venv first
.venv-dbt\Scripts\activate

cd dbt_crc
dbt run      # Build all models
dbt test     # Run data tests
dbt build    # Run + test
```

### Two venvs, two purposes

| venv | Python | Purpose |
|------|--------|---------|
| `.venv` | 3.14 | Main pipeline (`run_pipeline.py`) |
| `.venv-dbt` | 3.12 | dbt models (`dbt run`) |

Do NOT install dbt in the main `.venv` — it will fail.

## Dev/Prod Environments

**Default is always `dev`** so that running the pipeline or Power BI scripts without setting an environment uses development data and files by default.

### Setting the environment

- **Windows (Command Prompt):**  
  `set PIPELINE_ENV=prod`
- **Windows (PowerShell):**  
  `$env:PIPELINE_ENV="prod"`
- **macOS/Linux:**  
  `export PIPELINE_ENV=prod`

If `PIPELINE_ENV` is not set, it defaults to `dev`.

### run_pipeline.py `--env` flag

The pipeline uses **`datasets/{env}/{dataset_name}/pipeline.yaml`** when you pass `--dataset`:

```bash
# Dev (default): datasets/dev/tasks, datasets/dev/dept_mapping
python run_pipeline.py --dataset tasks
python run_pipeline.py --dataset dept_mapping

# Prod: datasets/prod/tasks, datasets/prod/dept_mapping
python run_pipeline.py --env prod --dataset tasks
python run_pipeline.py --env prod --dataset dept_mapping
```

`--env` accepts `dev` or `prod` (default: `dev`). The current environment is printed at pipeline start.

### Folder structure (dev vs prod)

| Environment | Dataset path | SQLite warehouse | DuckDB (Power BI) |
|-------------|---------------|-------------------|-------------------|
| **dev**     | `datasets/dev/tasks/`, `datasets/dev/dept_mapping/`, `datasets/dev/employees_master/` | `analytics/dev_warehouse.db` | `powerbi/dev_warehouse.duckdb` |
| **prod**    | `datasets/prod/tasks/`, `datasets/prod/dept_mapping/`, `datasets/prod/employees_master/` | `analytics/warehouse.db` | `powerbi/warehouse.duckdb` |

Place Excel files in the correct env folder, e.g. `datasets/dev/tasks/raw/` or `datasets/prod/tasks/raw/`.

### Power BI connection strings

Use the path that matches the environment you are using:

- **Dev (default):**
  ```
  Driver={DuckDB Driver};Database=C:\Users\quang\CRC Code\ExcelIngestion\powerbi\dev_warehouse.duckdb;access_mode=READ_ONLY
  ```
- **Prod:**
  ```
  Driver={DuckDB Driver};Database=C:\Users\quang\CRC Code\ExcelIngestion\powerbi\warehouse.duckdb;access_mode=READ_ONLY
  ```

Replace the path with your actual project path. Run `python powerbi/setup_odbc.py` (or `PIPELINE_ENV=prod python powerbi/setup_odbc.py` for prod) to print the exact path and connection string for the current environment.

---

## Project Structure

```
ExcelIngestion/
├── datasets/                   # Multi-dataset support
│   ├── dev/                    # Development environment (default)
│   │   ├── tasks/
│   │   │   ├── pipeline.yaml
│   │   │   ├── config/
│   │   │   ├── raw/            # Dev Excel files
│   │   │   ├── clean/, errors/, analytics/, logs/
│   │   ├── dept_mapping/
│   │   └── employees_master/   # Unified employee dimension (3 sources)
│   └── prod/                   # Production environment
│       ├── tasks/
│       ├── dept_mapping/
│       └── employees_master/
├── analytics/                  # Shared SQLite warehouse (project root)
│   ├── dev_warehouse.db        # Dev (when PIPELINE_ENV=dev)
│   └── warehouse.db            # Prod (when PIPELINE_ENV=prod)
├── powerbi/                    # DuckDB for Power BI ODBC
│   ├── dev_warehouse.duckdb    # Dev (default)
│   ├── warehouse.duckdb        # Prod
│   ├── create_duckdb.py
│   └── setup_odbc.py
├── lib/                        # Reusable pipeline functions
│   ├── convert.py
│   ├── normalize_schema.py
│   ├── clean_errors.py
│   ├── combine_datasets.py
│   ├── validate.py
│   ├── export_sqlite.py
│   └── ...
├── scripts/                    # Step scripts (thin wrappers around lib/)
│   ├── 01_convert.py
│   ├── 02_normalize_schema.py
│   └── ... (01-10)
├── tests/
│   ├── test_pipeline.py
│   └── fixtures/
├── run_pipeline.py             # Orchestrator (--pipeline flag)
└── requirements.txt
```

## Quick Start

```bash
# Setup
cd ExcelIngestion
python -m venv .venv
.venv\Scripts\activate          # Windows
source .venv/bin/activate       # macOS/Linux
pip install -r requirements.txt

# Run the tasks dataset (default)
python run_pipeline.py
# Or by name:
python run_pipeline.py --dataset tasks

# Run the employee/department mapping dataset
python run_pipeline.py --dataset dept_mapping

# Check output (dev: both datasets write to shared warehouse)
ls analytics/                      # dev_warehouse.db (dev) or warehouse.db (prod)
ls datasets/dev/tasks/analytics/   # combined.parquet
ls datasets/dev/tasks/logs/        # pipeline.log, validation_report.json
```

## Usage

### Run a Dataset Pipeline

```bash
# Default: runs datasets/dev/tasks/pipeline.yaml (--env dev)
python run_pipeline.py

# By dataset name (uses datasets/--env/NAME/pipeline.yaml)
python run_pipeline.py --dataset tasks
python run_pipeline.py --env prod --dataset tasks
python run_pipeline.py --dataset dept_mapping

# Explicit path to pipeline.yaml (overrides --env)
python run_pipeline.py --pipeline datasets/prod/tasks/pipeline.yaml
```

Both **tasks** and **dept_mapping** write to a **shared SQLite database** at `analytics/warehouse.db` (see [Shared warehouse](#shared-sqlite-warehouse) below). Each dataset uses its own table (`tasks` and `employees` respectively).

### Pipeline Options

```bash
# Environment: dev (default) or prod
python run_pipeline.py --env prod --dataset tasks

# Dry run - validate configs without processing
python run_pipeline.py --dry-run

# Start from a specific step (1-10)
python run_pipeline.py --from-step 6

# Combine options
python run_pipeline.py --env prod --dataset tasks --from-step 6
```

### Adding a New Dataset

1. Create the dataset folder structure:
```bash
mkdir -p datasets/my_dataset/{config,raw,clean,errors,analytics,logs}
```

2. Create `datasets/my_dataset/pipeline.yaml`:
```yaml
name: my_dataset

# Optional: SQLite export (defaults: database tasks.db, table tasks)
sqlite:
  database: warehouse.db   # Use shared warehouse (project analytics/)
  table: my_table          # Table name in the database
```
If `database: warehouse.db`, the table is written to **project** `analytics/warehouse.db`; otherwise to `datasets/my_dataset/analytics/<database>`.

3. Create config files in `datasets/my_dataset/config/`:
   - `schema.yaml` - Column definitions and validation rules
   - `combine.yaml` - Output filename
   - `value_maps.yaml` - Value normalization (optional)

4. Drop Excel files in `datasets/my_dataset/raw/`

5. Run the pipeline:
```bash
python run_pipeline.py --pipeline datasets/my_dataset/pipeline.yaml
```

## Pipeline Steps

| Step | Script | Description |
|------|--------|-------------|
| 01 | convert | Excel → Parquet |
| 02 | normalize_schema | Lowercase column names, apply column_aliases, reorder to schema |
| 03 | add_missing_columns | Add schema columns missing from source |
| 04 | clean_errors | Cast types, copy bad rows to `errors/` |
| 05 | normalize_values | Apply value_maps.yaml transformations |
| 06 | combine_datasets | Union all files, add `row_id` primary key |
| 07 | handle_nulls | Apply fill strategies from schema |
| 08 | validate | Check nulls, dtypes, row count; write JSON report |
| 09 | export_sqlite | Write to SQLite (table from pipeline.yaml); create/replace table only |
| 10 | sqlite_views | Sync dbt mart models as SQLite views for ad-hoc queries |

## Configuration

### schema.yaml

```yaml
# Optional: map source column names to canonical names (for multi-schema sources)
column_aliases:
  "Employee ID": "employee_id"
  "2025-2026 Hire": "is_recent_hire"
  "CRC Employee ID (Workday ID)": "employee_id"

columns:
  taskid:
    dtype: int64           # int64, float64, string, datetime64, bool
    nullable: false

  taskstatus:
    dtype: string
    nullable: true
    max_null_rate: 0.75    # Per-column null threshold

  dateavailable:
    dtype: datetime64
    nullable: true
    fill_strategy: null    # fill_zero, fill_forward, fill_backward, fill_unknown

validation:
  max_null_rate: 0.30      # Global null threshold
  max_duplicate_rate: 0.01 # Not used (row_id is always unique)
  min_row_count: 1

column_order:
  - taskid
  - drawer
  # ... remaining columns
```

**Column Aliases**: Use `column_aliases` when combining Excel files with different column headers. The pipeline applies aliases during step 02 (normalize_schema) to map source names to canonical names before further processing.

### combine.yaml

```yaml
# Primary key for validation duplicate check (step 06 adds row_id)
primary_key: row_id

# Output filename under dataset analytics/
output: combined.parquet
```

### value_maps.yaml

```yaml
taskstatus:
  "completed": "Completed"
  "COMPLETED": "Completed"
  "in progress": "In Progress"

drawer:
  "SCU  Bothell": "SCU Bothell"  # Fix double space
```

## Key Design Decisions

- **Surrogate key (`row_id`)**: Auto-generated 1-based integer. Allows duplicate business keys (taskid) across monthly snapshots.
- **Error rows copied, not filtered**: Bad rows go to `errors/` for inspection but don't block the pipeline.
- **Idempotent**: Rerunning processes everything from scratch.
- **Multi-dataset**: Each dataset is self-contained with its own config and data directories.
- **Shared warehouse**: Datasets can write to a single `analytics/warehouse.db` so tables can be joined in SQL and views.
- **Column aliasing**: Support for combining Excel files with different column headers via `column_aliases` in schema.yaml.

## Shared SQLite Warehouse

When `pipeline.yaml` sets `sqlite.database: warehouse.db`, step 09 writes to **project** `analytics/warehouse.db` instead of a per-dataset database. This allows multiple datasets to share one DB:

- **tasks** pipeline → table `tasks`
- **dept_mapping** pipeline → table `employees`
- **employees_master** pipeline → table `employees_master`

Step 10 syncs **dbt mart models** as SQLite views for ad-hoc queries. See [dbt Marts](#dbt-marts-analytics-layer) below.

Example queries:

```sql
SELECT COUNT(*) FROM tasks;
SELECT COUNT(*) FROM employees;
SELECT department, task_count FROM mart_team_demand WHERE task_week >= '2025-01-01';
SELECT drawer, completed_count, avg_handle_hours FROM mart_turnaround LIMIT 10;
```

## SQLite Database

```bash
# Shared warehouse (after running all datasets)
sqlite3 analytics/warehouse.db

# Tables: tasks, employees, employees_master
# Marts (synced from dbt): mart_tasks_enriched, mart_team_capacity, mart_team_demand,
#   mart_onshore_offshore, mart_backlog, mart_turnaround, mart_daily_trend
```

Per-dataset DBs (when `sqlite.database` is not `warehouse.db`) live under `datasets/<name>/analytics/<database>`.

## dbt Marts (Analytics Layer)

dbt is the single source of truth for analytics. Mart models are defined in `dbt_crc/models/marts/` and synced to both DuckDB (Power BI) and SQLite (ad-hoc queries).

| Mart | Description |
|------|-------------|
| `mart_tasks_enriched` | Tasks with worker fields, employee source, duration/lifecycle metrics |
| `mart_team_capacity` | Active headcount and FTE by cost center hierarchy and management level |
| `mart_team_demand` | Task volume and handle time by cost center and date (daily grain) |
| `mart_onshore_offshore` | Task metrics by employee source system, flow, and step |
| `mart_backlog` | Open tasks by drawer, flow, step, status with average age |
| `mart_turnaround` | Completed-task performance: counts and avg handle/lifecycle hours |
| `mart_daily_trend` | Daily opened vs completed by drawer with net backlog change |

### Running dbt

```bash
cd dbt_crc
dbt run      # Build all models
dbt test     # Run data tests
dbt build    # Run + test
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Generate test fixtures
python tests/create_fixtures.py --output-dir datasets/dev/tasks/raw
python tests/create_dept_fixtures.py --output-dir datasets/dev/dept_mapping/raw
```

## Power BI / DuckDB Integration

The pipeline creates a DuckDB database for Power BI ODBC consumption. **Default environment is dev** (creates `powerbi/dev_warehouse.duckdb`). For prod, set `PIPELINE_ENV=prod` before running so it creates/updates `powerbi/warehouse.duckdb`.

```bash
# Dev (default)
python powerbi/create_duckdb.py

# Prod
set PIPELINE_ENV=prod    # Windows
python powerbi/create_duckdb.py
```

**Tables created:**
| Table | Source | Rows |
|-------|--------|------|
| `tasks` | datasets/tasks/analytics/combined.parquet | 6M |
| `employees` | datasets/dept_mapping/analytics/combined.parquet | 200 |
| `employees_master` | datasets/employees_master/analytics/combined.parquet | 60 |

**Marts created (from dbt):**
| Mart | Description |
|------|-------------|
| `mart_tasks_enriched` | Tasks with worker fields, employee source, duration metrics |
| `mart_team_capacity` | Headcount and FTE by cost center hierarchy |
| `mart_team_demand` | Task volume by cost center and date |
| `mart_onshore_offshore` | Task metrics by employee source system |
| `mart_backlog` | Open tasks by drawer/flow/step with age |
| `mart_turnaround` | Completed-task handle/lifecycle hours |
| `mart_daily_trend` | Daily opened vs completed by drawer |

### DuckDB ODBC Setup (Windows)

1. Download and install [DuckDB ODBC Driver](https://duckdb.org/docs/api/odbc/overview)
2. Run `python powerbi/setup_odbc.py` (dev) or `set PIPELINE_ENV=prod` then `python powerbi/setup_odbc.py` to print the connection string for that environment
3. In Power BI: Get Data > ODBC > paste the connection string (see [Dev/Prod Environments](#devprod-environments) for dev vs prod paths)

## Troubleshooting

| Error | Solution |
|-------|----------|
| `Pipeline config not found` | Check --pipeline path points to a valid pipeline.yaml |
| `No Excel files in raw/` | Add .xlsx files to the dataset's raw/ folder |
| `Validation failed: null_rate` | Increase max_null_rate in schema.yaml |
| `ModuleNotFoundError` | Activate venv: `.venv\Scripts\activate` |
| `PyArrow schema mismatch` | Pipeline handles this automatically (all columns coerced to string) |
| `Power BI shows no tables` | Use DSN-less connection with explicit Database path |
| `ODBC connection failed` | Install DuckDB ODBC driver; check driver name matches |
