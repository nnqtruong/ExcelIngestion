# Excel Ingestion Pipeline - Current State (AI Context Document)

> **Purpose**: This document provides full context for LLMs to understand and assist with this codebase.
> **Last Updated**: 2025-03-24

---

## Project Overview

A 10-step data pipeline that converts Excel files into clean, validated Parquet datasets and exports to SQLite/DuckDB for Power BI consumption. Designed for CRC insurance industry data processing.

**Key Capabilities**:
- Handles 500K+ row Excel files with low memory usage (chunked processing)
- Processes 6M+ total rows across 12 files in production
- Dev/prod environment separation
- Multi-dataset support (tasks, dept_mapping, employees_master)
- Column aliasing for multi-schema source files
- Power BI integration via DuckDB ODBC

---

## Repository Structure

```
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
│   ├── sqlite_views.py          # Create analytics views
│   └── logging_util.py          # Logging configuration
│
├── scripts/                     # Step scripts (thin wrappers around lib/)
│   ├── 01_convert.py
│   ├── 02_normalize_schema.py
│   ├── 03_add_missing_columns.py
│   ├── 04_clean_errors.py
│   ├── 05_normalize_values.py
│   ├── 06_combine_datasets.py
│   ├── 07_handle_nulls.py
│   ├── 08_validate.py
│   ├── 09_export_sqlite.py
│   └── 10_sqlite_views.py
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
├── datasets/                    # Data organized by environment
│   ├── dev/                     # Development environment (default)
│   │   ├── tasks/
│   │   │   ├── pipeline.yaml    # Dataset configuration
│   │   │   ├── config/
│   │   │   │   ├── schema.yaml
│   │   │   │   ├── combine.yaml
│   │   │   │   └── value_maps.yaml
│   │   │   ├── raw/             # Input Excel files (*.xlsx)
│   │   │   ├── clean/           # Intermediate Parquet files
│   │   │   ├── errors/          # Rows that failed type casting
│   │   │   ├── analytics/       # combined.parquet output
│   │   │   └── logs/            # pipeline.log, validation_report.json
│   │   ├── dept_mapping/
│   │   │   ├── pipeline.yaml
│   │   │   ├── config/
│   │   │   ├── raw/
│   │   │   ├── clean/
│   │   │   ├── errors/
│   │   │   ├── analytics/
│   │   │   └── logs/
│   │   └── employees_master/    # Unified employee dimension (3 sources)
│   │       ├── pipeline.yaml
│   │       ├── config/          # schema.yaml with column_aliases
│   │       ├── raw/             # Brokerage.xlsx, Select.xlsx, Genpact.xlsx
│   │       ├── clean/
│   │       ├── errors/
│   │       ├── analytics/
│   │       └── logs/
│   └── prod/                    # Production environment
│       ├── tasks/               # Same structure as dev
│       └── dept_mapping/
│
├── analytics/                   # Shared SQLite warehouse
│   ├── dev_warehouse.db         # Dev environment (PIPELINE_ENV=dev)
│   └── warehouse.db             # Prod environment (PIPELINE_ENV=prod)
│
└── powerbi/                     # DuckDB files for Power BI ODBC
    ├── dev_warehouse.duckdb     # Dev (default)
    └── warehouse.duckdb         # Prod
```

---

## Pipeline Steps (1-10)

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
| 10 | sqlite_views | Sync dbt mart models as SQLite views for ad-hoc queries |

**Note**: dept_mapping uses a subset of steps (01, 02, 04, 05, 08, 09) defined in its pipeline.yaml.

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
| Dataset path | `datasets/dev/{dataset}/` | `datasets/prod/{dataset}/` |
| SQLite DB | `analytics/dev_warehouse.db` | `analytics/warehouse.db` |
| DuckDB (Power BI) | `powerbi/dev_warehouse.duckdb` | `powerbi/warehouse.duckdb` |

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

# Prod
python run_pipeline.py --env prod --dataset tasks

# Start from specific step
python run_pipeline.py --from-step 6

# Dry run (validate only)
python run_pipeline.py --dry-run
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

## dbt Marts (Step 10 - Analytics Layer)

dbt is the single source of truth for analytics. Mart SQL models are defined in `dbt_crc/models/marts/` and synced to both DuckDB (Power BI) and SQLite (ad-hoc queries) via `lib/sync_mart_views_sqlite.py`.

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

```bash
cd dbt_crc
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

3. **Idempotent**: Rerunning processes everything from scratch.

4. **Shared warehouse**: Both datasets write to single `analytics/warehouse.db` for cross-dataset joins.

5. **Environment separation**: Dev and prod have completely separate data directories and output files.

6. **Default is always dev**: Running without environment flags uses dev to prevent accidental production changes.

7. **Column aliasing**: Multi-schema sources (like employees_master) use `column_aliases` in schema.yaml to map different source column names to canonical names during step 02.

---

## Common Tasks for AI Assistance

1. **Add new column to tasks**: Update schema.yaml, value_maps.yaml (if needed), regenerate fixtures
2. **Add new dataset**: Create folder structure, pipeline.yaml, config files
2a. **Add multi-schema dataset**: Use `column_aliases` in schema.yaml (see employees_master)
3. **Modify validation rules**: Edit schema.yaml validation section
4. **Add/modify analytics mart**: Edit dbt_crc/models/marts/*.sql, run `dbt build`
5. **Update Power BI database**: Run `python powerbi/create_duckdb.py` after dbt changes
6. **Debug pipeline failures**: Check logs/ directory, errors/ directory

---

## File Paths Quick Reference

| What | Path |
|------|------|
| Main orchestrator | `run_pipeline.py` |
| Core logic | `lib/*.py` |
| Step scripts | `scripts/01-10_*.py` |
| Task schema | `datasets/{env}/tasks/config/schema.yaml` |
| Employee schema | `datasets/{env}/dept_mapping/config/schema.yaml` |
| Pipeline config | `datasets/{env}/{dataset}/pipeline.yaml` |
| Input Excel | `datasets/{env}/{dataset}/raw/*.xlsx` |
| Output Parquet | `datasets/{env}/{dataset}/analytics/combined.parquet` |
| SQLite warehouse | `analytics/{dev_}warehouse.db` |
| DuckDB (Power BI) | `powerbi/{dev_}warehouse.duckdb` |
| Pipeline logs | `datasets/{env}/{dataset}/logs/pipeline.log` |
| Validation report | `datasets/{env}/{dataset}/logs/validation_report.json` |
