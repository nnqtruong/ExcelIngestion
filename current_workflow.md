# Current Workflow - Data Flow Architecture

> **Purpose**: Visual and technical documentation of how data flows through the system.

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW DIAGRAM                                   │
└─────────────────────────────────────────────────────────────────────────────────┘

LAYER 1: INGESTION (Python Pipeline)
══════════════════════════════════════
    ┌──────────────┐
    │  Excel Files │  datasets/{env}/{dataset}/raw/*.xlsx
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
    │  datasets/{env}/{dataset}/analytics/combined.parquet     │
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
│analytics/warehouse.db│       ┌────────────────────────────┐
│ Tables:             │       │ dbt_crc/models/marts/      │
│  - tasks            │       │ ┌──────────────────────────┐│
│  - employees        │       │ │ mart_tasks_enriched.sql  ││
│ Views:              │       │ │ (3x employee JOINs)      ││
│  - v_task_duration  │       │ ├──────────────────────────┤│
│  - v_daily_volume   │       │ │ mart_daily_volume.sql    ││
│  - v_drawer_summary │       │ │ mart_drawer_performance  ││
│  - etc.             │       │ │ mart_team_workload.sql   ││
└─────────────────────┘       │ └──────────────────────────┘│
                              │ (GOLD LAYER - Business     │
                              │  metrics, aggregations)    │
                              └────────────┬───────────────┘
                                           │
                                           ▼
                              ┌────────────────────────────┐
                              │ powerbi/{env}_warehouse    │
                              │         .duckdb            │
                              │ Tables:                    │
                              │  - stg_tasks               │
                              │  - stg_employees           │
                              │  - mart_tasks_enriched     │
                              │  - mart_daily_volume       │
                              │  - mart_drawer_performance │
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
| **Raw** | Excel files | Manual drop | `datasets/{env}/{dataset}/raw/*.xlsx` |
| **Clean** | raw/*.xlsx | Python steps 1-5 | `datasets/{env}/{dataset}/clean/*.parquet` |
| **Combined** | clean/*.parquet | Python step 6 | `datasets/{env}/{dataset}/analytics/combined.parquet` |
| **SQLite** | combined.parquet | Python steps 9-10 | `analytics/{env_}warehouse.db` |
| **Staging** | combined.parquet | dbt staging models | DuckDB views (stg_*) |
| **Marts** | stg_* views | dbt mart models | DuckDB views (mart_*) |
| **Power BI** | DuckDB | ODBC connection | Reports |

---

## Layer Responsibilities

| Layer | Owned By | Purpose | Location |
|-------|----------|---------|----------|
| **Raw** | User | Drop Excel files | `datasets/{env}/{dataset}/raw/` |
| **Bronze** | Python | Convert, normalize schema | `datasets/{env}/{dataset}/clean/` |
| **Silver** | Python | Combine, validate | `datasets/{env}/{dataset}/analytics/combined.parquet` |
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

| Model | Type | Source | Description |
|-------|------|--------|-------------|
| `stg_tasks` | view | `combined.parquet` | Normalize join keys, apply value maps |
| `stg_employees` | view | `combined.parquet` | Clean employee data |
| `mart_tasks_enriched` | view | `stg_tasks` + `stg_employees` | 3x JOINs, computed columns |
| `mart_daily_volume` | view | `mart_tasks_enriched` | Daily task counts |
| `mart_drawer_performance` | view | `mart_tasks_enriched` | Drawer metrics |
| `mart_team_workload` | view | `mart_tasks_enriched` | Team workload metrics |

---

## Environment System

### Environment Variable: `PIPELINE_ENV`
- **Default**: `dev` (if not set)
- **Values**: `dev` or `prod`

### Path Resolution

| Component | Dev | Prod |
|-----------|-----|------|
| Dataset path | `datasets/dev/{dataset}/` | `datasets/prod/{dataset}/` |
| SQLite DB | `analytics/dev_warehouse.db` | `analytics/warehouse.db` |
| DuckDB | `powerbi/dev_warehouse.duckdb` | `powerbi/warehouse.duckdb` |

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

---

## Command Cheat Sheet

```bash
# Full pipeline refresh (dev)
python run_pipeline.py --dataset tasks
python run_pipeline.py --dataset dept_mapping
python run_pipeline.py --dataset employees_master
cd dbt_crc && dbt run && dbt test

# Full pipeline refresh (prod)
set PIPELINE_ENV=prod
python run_pipeline.py --env prod --dataset tasks
python run_pipeline.py --env prod --dataset dept_mapping
python run_pipeline.py --env prod --dataset employees_master
cd dbt_crc && dbt run --target prod

# Partial runs
python run_pipeline.py --dataset tasks --from-step 6   # Start from combine
dbt run --select stg_tasks+                            # Build stg_tasks and downstream

# Validation
python run_pipeline.py --dry-run
dbt test
python tests/compare_dbt_vs_pipeline.py
```
