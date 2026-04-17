# Excel Ingestion Pipeline - Current State (AI Context Document)

> **Purpose**: This document provides full context for LLMs to understand and assist with this codebase.
> **Last Updated**: 2026-04-17

---

## Project Overview

A 9-step data pipeline that converts Excel files into clean, validated Parquet datasets and exports to SQLite for Power BI consumption. dbt handles all analytics (staging views and marts) in DuckDB. Designed for CRC insurance industry data processing.

**Key Capabilities**:
- Handles 500K+ row Excel files with low memory usage (chunked processing)
- Processes 6M+ total rows across 12 files in production
- Dev/prod environment separation
- Multi-dataset support (tasks, dept_mapping, employees_master, workers, revenue)
- Column aliasing for multi-schema source files
- Power BI integration via DuckDB ODBC
- **Incremental processing** with file fingerprinting (skip unchanged files)
- **dbt analytics layer** with staging views and mart tables
- **External data directory** so user files and databases survive repo replace/zip upgrades

---

## External Data Directory

All **runtime** inputs and outputs use **`DATA_ROOT`** (environment variable). The default is **`../ExcelIngestion_Data`**, a folder **sibling to the `ExcelIngestion` repo** (same parent as the code).

| Area | Location |
|------|----------|
| Init (once) | `python scripts/init_data_directory.py` |
| Custom root | Set `DATA_ROOT` then run init (CMD: `set DATA_ROOT=D:\path`; PowerShell: `$env:DATA_ROOT="D:\path"`) |
| Migrate old in-repo `datasets/` | `python scripts/migrate_data.py --dry-run` then `python scripts/migrate_data.py` |
| Dataset layout | `{DATA_ROOT}/{env}/{dataset}/` with `raw/`, `clean/`, `analytics/`, `config/`, `logs/`, `_state/` |
| Combined Parquet | `{DATA_ROOT}/{env}/{dataset}/analytics/combined.parquet` |
| Shared SQLite | `{DATA_ROOT}/analytics/dev_warehouse.db` (dev) or `warehouse.db` (prod) |
| DuckDB (Power BI, dbt) | `{DATA_ROOT}/powerbi/dev_warehouse.duckdb` or `warehouse.duckdb` |

The in-repo **`datasets/`** tree holds **templates** copied by `init_data_directory.py`. Optional **backward compatibility:** `python run_pipeline.py --pipeline datasets/dev/tasks/pipeline.yaml` runs against the repo.

---

## Two Virtual Environments

This project requires **two separate Python virtual environments** because dbt does not support Python 3.14:

| venv | Python | Purpose | Activate |
|------|--------|---------|----------|
| `.venv` | 3.14 | Main pipeline (`run_pipeline.py`) | `.venv\Scripts\activate` |
| `.venv-dbt` | 3.12 | dbt models (`dbt run`) | `.venv-dbt\Scripts\activate` |

**Important**: Always activate the correct venv before running commands!

---

## Repository Structure

Runtime data lives **next to** the repo under **`ExcelIngestion_Data/`** by default (not inside `ExcelIngestion/`).

```
CRC Code/  (example parent)
├── ExcelIngestion/              # Git repo — code, tests, config templates
├── ExcelIngestion_Data/       # DATA_ROOT — Excel, Parquet, SQLite, DuckDB (not in git)
│   ├── dev/, prod/
│   ├── analytics/
│   └── powerbi/

ExcelIngestion/
├── run_pipeline.py              # Main orchestrator (--env, --dataset, --from-step)
├── requirements.txt             # Python dependencies
├── README.md                    # Technical documentation
├── USER_GUIDE.md                # Non-technical user guide
├── current_state.md             # This file (AI context)
│
├── lib/                         # Core pipeline logic (reusable functions)
│   ├── __init__.py
│   ├── paths.py                 # Path resolution, environment detection
│   ├── config.py                # YAML config loaders
│   ├── schema.py                # Schema loading, column aliases, validation rules
│   ├── convert.py               # Excel to Parquet conversion
│   ├── normalize_schema.py      # Column name normalization + aliasing
│   ├── add_missing_columns.py   # Add schema columns missing from source
│   ├── clean_errors.py          # Type casting, error row extraction
│   ├── normalize_values.py      # Value mapping transformations
│   ├── combine_datasets.py      # Union files, add row_id primary key
│   ├── handle_nulls.py          # Null fill strategies
│   ├── validate.py              # Validation checks, JSON report
│   ├── export_sqlite.py         # SQLite export
│   └── logging_util.py          # Logging configuration
│
├── scripts/                     # Step scripts (thin wrappers) + data layout helpers
│   ├── 01_convert.py
│   ├── 02_normalize_schema.py
│   ├── 03_add_missing_columns.py
│   ├── 04_clean_errors.py
│   ├── 05_normalize_values.py
│   ├── 06_combine_datasets.py
│   ├── 07_handle_nulls.py
│   ├── 08_validate.py
│   ├── 09_export_sqlite.py
│   ├── compare_schemas.py       # Schema comparison tool (--preflight)
│   ├── diagnose_schema.py       # Parquet schema diagnostics
│   ├── init_data_directory.py
│   └── migrate_data.py
│
├── powerbi/                     # Power BI integration
│   ├── create_duckdb.py         # Creates DuckDB from Parquet for ODBC
│   ├── setup_odbc.py            # Prints ODBC connection string
│   └── README.md
│
├── tests/                       # Test suite and fixtures
│   ├── test_pipeline.py         # Pytest tests
│   ├── create_fixtures.py       # Generate mock task Excel files
│   └── create_dept_fixtures.py  # Generate mock employee Excel file
│
├── .venv/                       # Main pipeline venv (Python 3.14)
├── .venv-dbt/                   # dbt venv (Python 3.12)
│
├── datasets/                    # Templates for init_data_directory.py (optional --pipeline runs)
│   ├── dev/
│   └── prod/
```

Under **`ExcelIngestion_Data/`** (default `DATA_ROOT`), each **`{env}/{dataset}/`** folder mirrors the old `datasets/` layout: `pipeline.yaml`, `config/`, `raw/`, `clean/`, `errors/`, `analytics/combined.parquet`, `logs/`, `_state/`. Shared **`analytics/`** holds SQLite; **`powerbi/`** holds DuckDB.

---

## Pipeline Steps (1-9)

| Step | Script | Description |
|------|--------|-------------|
| 01 | convert | Excel (.xlsx) → Parquet with chunked reading |
| 02 | normalize_schema | Lowercase columns, apply column_aliases, reorder to schema |
| 03 | add_missing_columns | Add schema columns missing from source files |
| 04 | clean_errors | Cast types, copy bad rows to `errors/` |
| 05 | normalize_values | Apply value_maps.yaml transformations |
| 06 | combine_datasets | Union all files, add `row_id` primary key |
| 07 | handle_nulls | Apply fill strategies from schema |
| 08 | validate | Check nulls, dtypes, row count; write JSON report |
| 09 | export_sqlite | Write to SQLite (shared warehouse.db) |

**Note**: Pipeline stops at step 09. dbt handles all analytics (staging views and marts) in DuckDB. Some datasets (like dept_mapping) use a subset of steps defined in their pipeline.yaml.

---

## Environment System

### Environment Variable: `PIPELINE_ENV`
- **Default**: `dev` (if not set)
- **Values**: `dev` or `prod`

### Command Line: `--env` flag
```bash
python run_pipeline.py --env dev --dataset tasks    # Dev (default)
python run_pipeline.py --env prod --dataset tasks   # Production
```

### Path Resolution

| Component | Dev | Prod |
|-----------|-----|------|
| Dataset path | `{DATA_ROOT}/dev/{dataset}/` | `{DATA_ROOT}/prod/{dataset}/` |
| SQLite DB | `{DATA_ROOT}/analytics/dev_warehouse.db` | `{DATA_ROOT}/analytics/warehouse.db` |
| DuckDB (Power BI) | `{DATA_ROOT}/powerbi/dev_warehouse.duckdb` | `{DATA_ROOT}/powerbi/warehouse.duckdb` |

Default `{DATA_ROOT}` = `../ExcelIngestion_Data`. Override with env var `DATA_ROOT` or `run_pipeline.py --data-root`.

---

## Datasets

### 1. Tasks Dataset
**Purpose**: CRC insurance task/workflow data from AMS system exports

**Schema** (23 columns):
| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| taskid | int64 | No | Business key (not unique across snapshots) |
| drawer | string | No | Office/department code |
| policynumber | string | No | |
| filename | string | Yes | Client name |
| filenumber | int64 | No | |
| effectivedate | datetime64 | No | |
| carrier | string | No | Insurance carrier |
| acctexec | string | Yes | Account executive |
| taskdescription | string | Yes | |
| assignedto | string | Yes | User ID |
| taskfrom | string | Yes | User ID |
| operationby | string | Yes | User ID |
| flowname | string | Yes | UW Renewal, UW New Business, Brokerage Support Processing |
| stepname | string | Yes | |
| sentto | string | Yes | |
| dateavailable | datetime64 | Yes | |
| dateinitiated | datetime64 | Yes | |
| dateended | datetime64 | Yes | |
| taskstatus | string | Yes | Completed, In Progress, Pending, Cancelled (75% null allowed) |
| starttime | datetime64 | Yes | |
| endtime | datetime64 | Yes | |
| _source_file | string | Yes | Auto-added: source Excel filename |
| _ingested_at | datetime64 | Yes | Auto-added: ingestion timestamp |

**SQLite**: Table `tasks` in shared warehouse.db

### 2. Dept_Mapping Dataset
**Purpose**: Employee to department/team mapping from HR export

**Schema** (13 columns):
| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| userid | string | No | Primary key (e.g., "ablain") |
| id | string | No | Alternate ID |
| full_name | string | No | |
| title | string | Yes | Job title |
| netwarelogin | string | Yes | e.g., "CRC\\ablain" |
| email | string | No | |
| divisionid | string | No | e.g., "CRCAL1" |
| division | string | No | e.g., "CRC - Birmingham" |
| division1 | string | Yes | Parent division |
| teamid | string | No | |
| team | string | No | |
| _source_file | string | Yes | Auto-added |
| _ingested_at | datetime64 | Yes | Auto-added |

**SQLite**: Table `employees` in shared warehouse.db

### 3. Employees_Master Dataset
**Purpose**: Unified employee dimension combining HR (Brokerage/Select) and Genpact data

**Source Files**:
- Brokerage.xlsx (20 rows, 15 columns)
- Select.xlsx (20 rows, 15 columns)
- Genpact.xlsx (20 rows, 7 columns)

**Schema** (24 columns):
| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| row_id | int64 | No | Surrogate key (auto-generated) |
| employee_id | string | No | Unified from multiple source columns |
| name | string | No | |
| supervisory_organization | string | Yes | HR sources only |
| job_profile | string | Yes | HR sources only |
| cost_center | string | Yes | HR sources only |
| hire_date | datetime64 | Yes | |
| is_recent_hire | string | Yes | |
| location | string | Yes | |
| term_date | datetime64 | Yes | |
| is_recent_term | string | Yes | |
| term_reason | string | Yes | |
| role_disposition | string | Yes | |
| combined_hierarchy | string | Yes | |
| team_or_office | string | Yes | Aliased from "Brokerage Production team" or "Office" |
| addressable_population | string | Yes | |
| email | string | Yes | Genpact source only |
| genpact_id | string | Yes | Genpact source only |
| genpact_phase | string | Yes | Genpact source only |
| genpact_crc_name | string | Yes | Genpact source only |
| genpact_mapping | string | Yes | Genpact source only |
| source_system | string | No | Derived from _source_file (Brokerage/Select/Genpact) |
| _source_file | string | Yes | Auto-added |
| _ingested_at | datetime64 | Yes | Auto-added |

**Key Feature**: Uses `column_aliases` in schema.yaml to map different source column names to canonical names.

**SQLite**: Table `employees_master` in shared warehouse.db

---

## Configuration Files

### pipeline.yaml (per dataset)
```yaml
name: tasks
environment: dev

sqlite:
  database: warehouse.db   # Use shared warehouse
  table: tasks             # Table name

# Optional: limit which steps run
steps:
  - 01_convert
  - 02_normalize_schema
  # ...
```

### schema.yaml (per dataset)
Defines columns, data types, nullable constraints, and validation thresholds.

**Column Aliases** (optional):
```yaml
column_aliases:
  "Employee ID": "employee_id"
  "CRC Employee ID (Workday ID)": "employee_id"
  "2025-2026 Hire": "is_recent_hire"
  "2025-26 Hire": "is_recent_hire"
```
Used when combining Excel files with different column headers. Applied in step 02.

### combine.yaml
```yaml
primary_key: row_id        # Surrogate key added in step 06
output: combined.parquet   # Output filename
```

### value_maps.yaml
```yaml
taskstatus:
  "completed": "Completed"
  "COMPLETED": "Completed"
  "in progress": "In Progress"
```

---

## Key Commands

### Run Pipeline
```bash
# Dev (default)
python run_pipeline.py --dataset tasks
python run_pipeline.py --dataset dept_mapping
python run_pipeline.py --dataset employees_master

# Run all datasets
python run_pipeline.py --all

# Prod
python run_pipeline.py --env prod --dataset tasks

# Start from specific step
python run_pipeline.py --from-step 6

# Dry run (validate only)
python run_pipeline.py --dry-run

# Preflight schema check (recommended before running)
python run_pipeline.py --dataset tasks --preflight
python run_pipeline.py --all --preflight --dry-run
```

### Schema Comparison Tool
```bash
# Compare Excel headers across all files in a dataset
python scripts/compare_schemas.py --dataset tasks --env dev

# Check against schema.yaml (shows aliases, missing/extra columns)
python scripts/compare_schemas.py --dataset tasks --env dev --check-against

# Compare all files against a specific baseline
python scripts/compare_schemas.py --dataset tasks --env dev --baseline "Jan 2025.xlsx"

# Export full report as JSON
python scripts/compare_schemas.py --dataset tasks --env dev --output schema_report.json
```

### Generate Test Data
```bash
# Task fixtures (12 Excel files, 100 rows each = 1,200 total)
python tests/create_fixtures.py --scale small --output-dir datasets/dev/tasks/raw

# Employee fixtures (1 Excel file, 200 rows)
python tests/create_dept_fixtures.py --output-dir datasets/dev/dept_mapping/raw
```

### Create Power BI Database
```bash
# Dev (default)
python powerbi/create_duckdb.py

# Prod
set PIPELINE_ENV=prod
python powerbi/create_duckdb.py
```

### Run Tests
```bash
pytest tests/ -v
```

---

## dbt Marts (Analytics Layer)

dbt is the single source of truth for analytics. Mart SQL models are defined in `dbt_crc/models/marts/` and built in DuckDB. SQLite contains only base tables; all staging views and marts live in DuckDB.

| Mart | Description |
|------|-------------|
| `mart_tasks_enriched` | Tasks with Workday worker fields, employee source, duration/lifecycle metrics |
| `mart_team_capacity` | Active headcount and FTE by cost center hierarchy and management level |
| `mart_team_demand` | Task volume and handle time by cost center and date (daily grain) |
| `mart_onshore_offshore` | Task metrics by employee source system, flow, and step |
| `mart_backlog` | Open tasks by drawer, flow, step, status with average age |
| `mart_turnaround` | Completed-task performance: counts and avg handle/lifecycle hours |
| `mart_daily_trend` | Daily opened vs completed by drawer with net backlog change |

### Running dbt

**Important**: dbt requires Python 3.12, not the main venv's Python 3.14.

```bash
# Activate dbt venv first
.venv-dbt\Scripts\activate

cd dbt_crc
dbt deps     # Install packages (first time only)
dbt run      # Build all models
dbt test     # Run data tests
dbt build    # Run + test
```

---

## DuckDB (Power BI)

### Base Tables
| Table | Source |
|-------|--------|
| `tasks` | tasks/analytics/combined.parquet |
| `employees` | dept_mapping/analytics/combined.parquet |
| `employees_master` | employees_master/analytics/combined.parquet |

### dbt Marts
| Mart | Description |
|------|-------------|
| `mart_tasks_enriched` | Tasks with worker fields, employee source, duration metrics |
| `mart_team_capacity` | Headcount and FTE by cost center hierarchy |
| `mart_team_demand` | Task volume by cost center and date |
| `mart_onshore_offshore` | Task metrics by employee source system |
| `mart_backlog` | Open tasks by drawer/flow/step with age |
| `mart_turnaround` | Completed-task handle/lifecycle hours |
| `mart_daily_trend` | Daily opened vs completed by drawer |

---

## Data Quality Issues Handled

The fixture generators inject realistic data quality issues that the pipeline cleans:

**Tasks**:
- Null values in non-critical fields
- Bad casing in flowname (lowercase)
- Bad casing in TaskStatus (uppercase)
- Double spaces in Drawer names
- Files 7-12 missing TaskStatus column entirely

**Dept_Mapping**:
- Literal "NULL" strings in title
- Case variations in netwarelogin (crc\\, Crc\\, CRC\\)
- Trailing whitespace in Full Name
- Empty strings in title
- Malformed emails
- Duplicate userids

---

## Dependencies (requirements.txt)

```
pandas
pyarrow
openpyxl
pyyaml
pytest
duckdb
psutil
pyodbc
```

---

## Key Design Decisions

1. **Surrogate key (row_id)**: Auto-generated 1-based integer in step 06. Allows duplicate business keys (taskid) across monthly snapshots.

2. **Error rows copied, not filtered**: Bad rows go to `errors/` directory for inspection but don't block the pipeline.

3. **Incremental with fingerprinting**: File MD5 hashes stored in `_state/ingestion_state.json`. Step 01 skips unchanged files. Use `--force` to reprocess all.

4. **Shared warehouse**: All datasets write to a single SQLite file under `{DATA_ROOT}/analytics/` (`dev_warehouse.db` or `warehouse.db`) for cross-dataset joins.

5. **Environment separation**: Dev and prod have completely separate trees under `DATA_ROOT`.

6. **Default is always dev**: Running without environment flags uses dev to prevent accidental production changes.

7. **Column aliasing**: Multi-schema sources (like employees_master) use `column_aliases` in schema.yaml to map different source column names to canonical names during step 02.

8. **Two Python venvs**: Main pipeline uses Python 3.14 (`.venv`), dbt uses Python 3.12 (`.venv-dbt`) due to dbt compatibility requirements.

9. **Windows DuckDB workaround**: Write to temp file then `shutil.move()` to avoid "Access is denied" file locking errors.

10. **External data directory**: User data and databases live under `DATA_ROOT` (default sibling `ExcelIngestion_Data/`), not inside the git repo, so code can be replaced via zip without touching Excel, Parquet, or warehouses. `init_data_directory.py` seeds layout from `datasets/`; `migrate_data.py` moves legacy in-repo files.

---

## Common Tasks for AI Assistance

1. **Add new column to tasks**: Update schema.yaml, value_maps.yaml (if needed), regenerate fixtures
2. **Add new dataset**: Create folder structure, pipeline.yaml, config files
2a. **Add multi-schema dataset**: Use `column_aliases` in schema.yaml (see employees_master)
3. **Modify validation rules**: Edit schema.yaml validation section
4. **Add/modify analytics mart**: Edit dbt_crc/models/marts/*.sql, run `dbt build`
5. **Update Power BI database**: Run `python powerbi/create_duckdb.py` after dbt changes
6. **Debug pipeline failures**: Check logs/ directory, errors/ directory
7. **Check schema drift before running**: Run `python scripts/compare_schemas.py --dataset NAME --check-against`
8. **Preflight check all datasets**: Run `python run_pipeline.py --all --preflight --dry-run`

---

## File Paths Quick Reference

`{DATA_ROOT}` defaults to **`../ExcelIngestion_Data`**. Template copies in git remain under **`datasets/`** for `init_data_directory.py`.

| What | Path |
|------|------|
| Main orchestrator | `run_pipeline.py` |
| Core logic | `lib/*.py` |
| Step scripts | `scripts/01_*.py` … `scripts/09_*.py` |
| Init external layout | `scripts/init_data_directory.py` |
| Migrate legacy data | `scripts/migrate_data.py` |
| Schema / pipeline (runtime) | `{DATA_ROOT}/{env}/{dataset}/config/schema.yaml`, `pipeline.yaml` |
| Input Excel | `{DATA_ROOT}/{env}/{dataset}/raw/*.xlsx` |
| Output Parquet | `{DATA_ROOT}/{env}/{dataset}/analytics/combined.parquet` |
| SQLite warehouse | `{DATA_ROOT}/analytics/dev_warehouse.db` or `warehouse.db` |
| DuckDB (Power BI, dbt) | `{DATA_ROOT}/powerbi/dev_warehouse.duckdb` or `warehouse.duckdb` |
| Pipeline logs | `{DATA_ROOT}/{env}/{dataset}/logs/pipeline.log` |
| Validation report | `{DATA_ROOT}/{env}/{dataset}/logs/validation_report.json` |
