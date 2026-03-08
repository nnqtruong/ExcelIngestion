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

Place Excel files in the dataset's `raw/` folder:

```
datasets/tasks/raw/           # Task data (*.xlsx)
datasets/dept_mapping/raw/    # Employee/department mapping (*.xlsx)
```

### 4. Run the Pipeline

```bash
# Run tasks dataset (default)
python run_pipeline.py

# Run dept_mapping dataset
python run_pipeline.py --dataset dept_mapping

# Dry run (validate configs only)
python run_pipeline.py --dry-run
```

### 5. Connect Power BI via DuckDB ODBC

```bash
# Create DuckDB database for Power BI
python powerbi/create_duckdb.py

# Test ODBC connection
python powerbi/setup_odbc.py
```

In Power BI:
1. Get Data > Other > ODBC
2. Use DSN-less connection string:
   ```
   Driver={DuckDB Driver};Database=C:\Users\quang\CRC Code\ExcelIngestion\powerbi\warehouse.duckdb;access_mode=READ_ONLY
   ```

## Project Structure

```
ExcelIngestion/
├── datasets/                   # Multi-dataset support
│   ├── tasks/                  # Task tracking dataset
│   │   ├── pipeline.yaml       # Dataset marker file
│   │   ├── config/
│   │   │   ├── schema.yaml     # Column definitions, types, validation
│   │   │   ├── value_maps.yaml # Value normalization mappings
│   │   │   └── combine.yaml    # Output filename
│   │   ├── raw/                # Input Excel files
│   │   ├── clean/              # Intermediate Parquet files
│   │   ├── errors/             # Rows that failed type casting
│   │   ├── analytics/          # Dataset output (combined.parquet)
│   │   └── logs/               # pipeline.log, validation_report.json
│   └── dept_mapping/           # Another dataset (same structure)
├── analytics/                  # Shared SQLite warehouse (project root)
│   └── warehouse.db            # tasks + employees tables; cross-dataset views
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

# Check output (both datasets write to shared warehouse)
ls analytics/                   # warehouse.db (tasks + employees tables)
ls datasets/tasks/analytics/    # combined.parquet
ls datasets/tasks/logs/         # pipeline.log, validation_report.json
```

## Usage

### Run a Dataset Pipeline

```bash
# Default: runs datasets/tasks/pipeline.yaml
python run_pipeline.py

# By dataset name (uses datasets/NAME/pipeline.yaml)
python run_pipeline.py --dataset tasks
python run_pipeline.py --dataset dept_mapping

# Explicit path to pipeline.yaml
python run_pipeline.py --pipeline datasets/tasks/pipeline.yaml
python run_pipeline.py --pipeline datasets/dept_mapping/pipeline.yaml
```

Both **tasks** and **dept_mapping** write to a **shared SQLite database** at `analytics/warehouse.db` (see [Shared warehouse](#shared-sqlite-warehouse) below). Each dataset uses its own table (`tasks` and `employees` respectively).

### Pipeline Options

```bash
# Dry run - validate configs without processing
python run_pipeline.py --dry-run

# Start from a specific step (1-10)
python run_pipeline.py --from-step 6

# Combine options
python run_pipeline.py --pipeline datasets/tasks/pipeline.yaml --from-step 6
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
| 02 | normalize_schema | Lowercase column names, reorder to schema |
| 03 | add_missing_columns | Add schema columns missing from source |
| 04 | clean_errors | Cast types, copy bad rows to `errors/` |
| 05 | normalize_values | Apply value_maps.yaml transformations |
| 06 | combine_datasets | Union all files, add `row_id` primary key |
| 07 | handle_nulls | Apply fill strategies from schema |
| 08 | validate | Check nulls, dtypes, row count; write JSON report |
| 09 | export_sqlite | Write to SQLite (table from pipeline.yaml); create/replace table only |
| 10 | sqlite_views | Create task analytics views + cross-dataset view if both tables exist |

## Configuration

### schema.yaml

```yaml
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

## Shared SQLite Warehouse

When `pipeline.yaml` sets `sqlite.database: warehouse.db`, step 09 writes to **project** `analytics/warehouse.db` instead of a per-dataset database. This allows multiple datasets to share one DB:

- **tasks** pipeline → table `tasks`
- **dept_mapping** pipeline → table `employees`

Step 10 creates a **cross-dataset view** when both tables exist:

- **`v_tasks_by_department`** – tasks LEFT JOIN employees on `LOWER(assignedto)=LOWER(userid)` OR `LOWER(operationby)=LOWER(userid)` OR `LOWER(taskfrom)=LOWER(userid)`, adding `full_name`, `employee_title`, `division`, `team`, `divisionid` from employees. Use it for task counts by division or to see which employee a task is assigned to.

Example queries:

```sql
SELECT COUNT(*) FROM tasks;
SELECT COUNT(*) FROM employees;
SELECT division, COUNT(*) AS task_count FROM v_tasks_by_department GROUP BY division;
SELECT * FROM v_tasks_by_department WHERE full_name IS NOT NULL LIMIT 10;
```

## SQLite Database

```bash
# Shared warehouse (after running both tasks and dept_mapping)
sqlite3 analytics/warehouse.db

# Tables: tasks, employees
# Task-only views (require tasks table): v_task_duration, v_daily_volume, v_drawer_summary, v_carrier_workload, v_missing_status
# Cross-dataset view: v_tasks_by_department
```

Per-dataset DBs (when `sqlite.database` is not `warehouse.db`) live under `datasets/<name>/analytics/<database>`.

## Testing

```bash
# Run all tests
pytest tests/ -v

# Generate test fixtures (tasks: project raw/; copy to datasets/tasks/raw for pipeline)
python tests/create_fixtures.py
python tests/create_dept_fixtures.py   # writes datasets/dept_mapping/raw/employee_mapping.xlsx
```

## Power BI / DuckDB Integration

The pipeline creates a DuckDB database for Power BI ODBC consumption:

```bash
python powerbi/create_duckdb.py
```

**Tables created:**
| Table | Source | Rows |
|-------|--------|------|
| `tasks` | datasets/tasks/analytics/combined.parquet | 6M |
| `employees` | datasets/dept_mapping/analytics/combined.parquet | 200 |
| `tasks_with_dept` | JOIN tasks + employees on `LOWER(TRIM(operationby))` | 6M |

**Views created:**
| View | Description |
|------|-------------|
| `v_task_duration` | Tasks with duration_minutes, duration_hours, lifecycle_hours |
| `v_daily_volume` | Task counts by date (initiated, completed, in_progress, pending) |
| `v_drawer_summary` | Task counts by drawer with avg duration |
| `v_carrier_workload` | Task counts by carrier and flowname |
| `v_missing_status` | Tasks where taskstatus IS NULL |
| `v_tasks_by_department` | Tasks LEFT JOIN employees |
| `v_team_workload` | Task counts by team and division |

### DuckDB ODBC Setup (Windows)

1. Download and install [DuckDB ODBC Driver](https://duckdb.org/docs/api/odbc/overview)
2. Run `python powerbi/setup_odbc.py` to test connection
3. In Power BI: Get Data > ODBC > paste connection string

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
