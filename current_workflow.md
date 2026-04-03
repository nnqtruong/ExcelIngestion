# Current Workflow - Data Flow Architecture

> **Purpose**: Visual and technical documentation of how data flows through the system.

**`DATA_ROOT`:** Set the environment variable `DATA_ROOT` to your external data folder (default: **`ExcelIngestion_Data`** next to the **`ExcelIngestion`** repo). All paths below use `{DATA_ROOT}`; they are **not** under the git working tree unless you use `--pipeline datasets/...`.

```
Code vs data (default layout)
─────────────────────────────────────────────────────────
  ExcelIngestion\          ← Repository (clone / zip updates)
  ExcelIngestion_Data\     ← DATA_ROOT (persistent user data)
─────────────────────────────────────────────────────────
```

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW DIAGRAM                                   │
└─────────────────────────────────────────────────────────────────────────────────┘

LAYER 1: INGESTION (Python Pipeline)
══════════════════════════════════════
    ┌──────────────┐
    │  Excel Files │  {DATA_ROOT}/{env}/{dataset}/raw/*.xlsx
    │  (Source)    │
    └──────┬───────┘
           │
           ▼
    ┌──────────────────────────────────────────────────────────┐
    │  Python Pipeline (run_pipeline.py)                       │
    │  ┌────────┬────────┬────────┬────────┬────────┬────────┐ │
    │  │ Step 1 │ Step 2 │ Step 3 │ Step 4 │ Step 5 │ Step 6 │ │
    │  │Convert │Normalize│Add Cols│ Clean  │Norm Val│Combine │ │
    │  │xlsx→pq │ Schema │Missing │ Errors │  Maps  │ Union  │ │
    │  └────────┴────────┴────────┴────────┴────────┴────────┘ │
    └──────────────────────────┬───────────────────────────────┘
                               │
                               ▼
    ┌──────────────────────────────────────────────────────────┐
    │  {DATA_ROOT}/{env}/{dataset}/analytics/combined.parquet   │
    │  (BRONZE/SILVER LAYER - Clean, typed, combined data)     │
    └──────────────────────────┬───────────────────────────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
        ▼                      ▼                      ▼

LAYER 2A: SQLITE (Python)     LAYER 2B: dbt-DuckDB (SQL)
═══════════════════════       ═══════════════════════════
┌─────────────────────┐       ┌────────────────────────────┐
│ Steps 7-10          │       │ dbt_crc/models/staging/    │
│ ┌───────┬─────────┐ │       │ ┌────────────┬───────────┐ │
│ │Step 7 │ Step 8  │ │       │ │stg_tasks   │stg_employ │ │
│ │Nulls  │Validate │ │       │ │  .sql      │  ees.sql  │ │
│ ├───────┼─────────┤ │       │ └─────┬──────┴─────┬─────┘ │
│ │Step 9 │ Step 10 │ │       │       │            │       │
│ │SQLite │ Views   │ │       │       ▼            ▼       │
│ └───────┴─────────┘ │       │ (Staging Layer - normalize,│
└─────────┬───────────┘       │  join keys, value maps)    │
          │                   └────────────┬───────────────┘
          ▼                                │
┌─────────────────────┐                    ▼
│{DATA_ROOT}/analytics/*.db │       ┌────────────────────────────┐
│ Tables:             │       │ dbt_crc/models/marts/      │
│  - tasks            │       │ ┌──────────────────────────┐│
│  - employees        │       │ │ mart_tasks_enriched.sql  ││
│  - employees_master │       │ │ mart_team_capacity.sql   ││
│ Marts (synced):     │       │ │ mart_team_demand.sql     ││
│  - mart_tasks_      │       │ │ mart_onshore_offshore    ││
│      enriched       │       │ │ mart_backlog.sql         ││
│  - mart_team_demand │       │ │ mart_turnaround.sql      ││
│  - etc.             │       │ │ mart_daily_trend.sql     ││
└─────────────────────┘       │ └──────────────────────────┘│
                              │ (GOLD LAYER - Business     │
                              │  metrics, aggregations)    │
                              └────────────┬───────────────┘
                                           │
                                           ▼
                              ┌────────────────────────────┐
                              │ {DATA_ROOT}/powerbi/       │
                              │   {env}_warehouse.duckdb   │
                              │ Tables:                    │
                              │  - tasks, employees,       │
                              │    employees_master        │
                              │ Marts:                     │
                              │  - mart_tasks_enriched     │
                              │  - mart_team_capacity      │
                              │  - mart_team_demand        │
                              │  - mart_onshore_offshore   │
                              │  - mart_backlog            │
                              │  - mart_turnaround         │
                              │  - mart_daily_trend        │
                              │  - mart_team_workload      │
                              └────────────┬───────────────┘
                                           │
                                           ▼
                              ┌────────────────────────────┐
                              │      Power BI (ODBC)       │
                              │   Reports & Dashboards     │
                              └────────────────────────────┘
```

---

## File Flow Summary

| Stage | Input | Process | Output |
|-------|-------|---------|--------|
| **Raw** | Excel files | Manual drop | `{DATA_ROOT}/{env}/{dataset}/raw/*.xlsx` |
| **Clean** | raw/*.xlsx | Python steps 1-5 | `{DATA_ROOT}/{env}/{dataset}/clean/*.parquet` |
| **Combined** | clean/*.parquet | Python step 6 | `{DATA_ROOT}/{env}/{dataset}/analytics/combined.parquet` |
| **SQLite** | combined.parquet | Python steps 9-10 | `{DATA_ROOT}/analytics/{env_}warehouse.db` |
| **Staging** | combined.parquet | dbt staging models | DuckDB views (stg_*) |
| **Marts** | stg_* views | dbt mart models | DuckDB views (mart_*) |
| **Power BI** | DuckDB | ODBC connection | Reports |

---

## Layer Responsibilities

| Layer | Owned By | Purpose | Location |
|-------|----------|---------|----------|
| **Raw** | User | Drop Excel files | `{DATA_ROOT}/{env}/{dataset}/raw/` |
| **Bronze** | Python | Convert, normalize schema | `{DATA_ROOT}/{env}/{dataset}/clean/` |
| **Silver** | Python | Combine, validate | `{DATA_ROOT}/{env}/{dataset}/analytics/combined.parquet` |
| **Staging** | dbt | Normalize values, prepare joins | `dbt_crc/models/staging/stg_*.sql` |
| **Gold/Marts** | dbt | Business metrics, aggregations | `dbt_crc/models/marts/mart_*.sql` |
| **Presentation** | Power BI | Dashboards, reports | ODBC → DuckDB |

---

## Pipeline Steps Detail

### Python Pipeline (Steps 1-10)

| Step | Script | Input | Output | Description |
|------|--------|-------|--------|-------------|
| 01 | convert | `raw/*.xlsx` | `clean/*.parquet` | Excel to Parquet with chunked reading |
| 02 | normalize_schema | `clean/*.parquet` | `clean/*.parquet` | Lowercase columns, apply column_aliases, reorder to schema |
| 03 | add_missing_columns | `clean/*.parquet` | `clean/*.parquet` | Add schema columns missing from source |
| 04 | clean_errors | `clean/*.parquet` | `clean/*.parquet` + `errors/*.parquet` | Cast types, extract bad rows |
| 05 | normalize_values | `clean/*.parquet` | `clean/*.parquet` | Apply value_maps.yaml transformations |
| 06 | combine_datasets | `clean/*.parquet` | `analytics/combined.parquet` | Union all files, add row_id |
| 07 | handle_nulls | `analytics/combined.parquet` | `analytics/combined.parquet` | Apply fill strategies |
| 08 | validate | `analytics/combined.parquet` | `logs/validation_report.json` | Check nulls, dtypes, row count |
| 09 | export_sqlite | `analytics/combined.parquet` | `analytics/warehouse.db` | Write to SQLite |
| 10 | sqlite_views | `analytics/warehouse.db` | `analytics/warehouse.db` | Create analytics views |

### dbt Models

**Staging Models:**
| Model | Source | Description |
|-------|--------|-------------|
| `stg_tasks` | tasks/combined.parquet | Normalize join keys, apply value maps |
| `stg_employees` | dept_mapping/combined.parquet | Clean employee data |
| `stg_workers` | workers/combined.parquet | Workday worker data |
| `stg_employees_master` | employees_master/combined.parquet | Unified employee dimension |

**Mart Models:**
| Model | Description |
|-------|-------------|
| `mart_tasks_enriched` | Tasks with worker fields, employee source, duration metrics |
| `mart_team_capacity` | Headcount and FTE by cost center hierarchy |
| `mart_team_demand` | Task volume by cost center and date (daily) |
| `mart_onshore_offshore` | Task metrics by employee source system |
| `mart_backlog` | Open tasks by drawer/flow/step with age |
| `mart_turnaround` | Completed-task handle/lifecycle hours |
| `mart_daily_trend` | Daily opened vs completed by drawer |

---

## Environment System

### Environment Variable: `PIPELINE_ENV`
- **Default**: `dev` (if not set)
- **Values**: `dev` or `prod`

### Path Resolution

| Component | Dev | Prod |
|-----------|-----|------|
| Dataset path | `{DATA_ROOT}/dev/{dataset}/` | `{DATA_ROOT}/prod/{dataset}/` |
| SQLite DB | `{DATA_ROOT}/analytics/dev_warehouse.db` | `{DATA_ROOT}/analytics/warehouse.db` |
| DuckDB | `{DATA_ROOT}/powerbi/dev_warehouse.duckdb` | `{DATA_ROOT}/powerbi/warehouse.duckdb` |

---

## Current Datasets

### 1. Tasks
- **Source**: CRC AMS system task exports
- **Files**: 12 monthly Excel files (~500K rows each)
- **Primary Key**: `row_id` (surrogate, auto-generated)
- **SQLite Table**: `tasks`
- **dbt Models**: `stg_tasks` → `mart_tasks_enriched` → aggregation marts

### 2. Dept_Mapping
- **Source**: HR employee/department export
- **Files**: 1 Excel file (~200 rows)
- **Primary Key**: `userid`
- **SQLite Table**: `employees`
- **dbt Models**: `stg_employees` (joined to tasks)

### 3. Employees_Master
- **Source**: Unified HR + Genpact employee dimension
- **Files**: 3 Excel files with different schemas (Brokerage.xlsx, Select.xlsx, Genpact.xlsx)
- **Primary Key**: `row_id` (surrogate)
- **SQLite Table**: `employees_master`
- **Key Feature**: Uses `column_aliases` in schema.yaml to unify different column names
- **Columns**: 24 (including source_system to track row origin)

### 4. Workers
- **Source**: Workday HR worker export
- **Files**: 1 Excel file (~4K rows)
- **Primary Key**: `employee_id`
- **SQLite Table**: `workers`
- **dbt Models**: `stg_workers` → `mart_team_capacity`

### 5. Revenue
- **Source**: Monthly revenue by broker/team/division
- **Files**: 1 Excel file (Revenue.xlsx)
- **Primary Key**: `row_id` (surrogate)
- **SQLite Table**: `revenue`
- **Columns**: year, quarter, month, subdivision, division_name, team_name, broker_name, revenue

---

## Two Virtual Environments Required

| venv | Python | Purpose | Activate |
|------|--------|---------|----------|
| `.venv` | 3.14 | Main pipeline (`run_pipeline.py`) | `.venv\Scripts\activate` |
| `.venv-dbt` | 3.12 | dbt models (`dbt run`) | `.venv-dbt\Scripts\activate` |

**Important**: dbt does NOT support Python 3.14. Always use `.venv-dbt` for dbt commands!

---

## Command Cheat Sheet

```bash
# First-time or new machine: create external data folder (default ../ExcelIngestion_Data)
python scripts/init_data_directory.py

# Optional: custom data location (CMD)
set DATA_ROOT=D:\MyData\ExcelIngestion
python scripts/init_data_directory.py

# Full pipeline refresh (dev) — use .venv
.venv\Scripts\activate
python run_pipeline.py --dataset tasks
python run_pipeline.py --dataset dept_mapping
python run_pipeline.py --dataset employees_master
python run_pipeline.py --dataset workers
python run_pipeline.py --dataset revenue

# Then run dbt — switch to .venv-dbt
.venv-dbt\Scripts\activate
cd dbt_crc && dbt run && dbt test

# Full pipeline refresh (prod)
.venv\Scripts\activate
set PIPELINE_ENV=prod
python run_pipeline.py --env prod --dataset tasks
python run_pipeline.py --env prod --dataset dept_mapping
python run_pipeline.py --env prod --dataset employees_master

.venv-dbt\Scripts\activate
cd dbt_crc && dbt run --target prod

# Partial runs
python run_pipeline.py --dataset tasks --from-step 6   # Start from combine
python run_pipeline.py --dataset tasks --force          # Reprocess all files (ignore fingerprints)
dbt run --select stg_tasks+                            # Build stg_tasks and downstream

# Validation
python run_pipeline.py --dry-run
dbt test
python tests/compare_dbt_vs_pipeline.py
```
