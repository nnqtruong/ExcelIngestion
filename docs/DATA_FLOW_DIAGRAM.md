# ExcelIngestion Pipeline - Data Flow Visualization

**Layout:** **`ExcelIngestion/`** holds code (git). **`ExcelIngestion_Data/`** is the default **`DATA_ROOT`**: Excel, Parquet, SQLite, and DuckDB live here so they are not overwritten by repo updates. Set **`DATA_ROOT`** to override the folder name or location.

## Complete Pipeline Architecture

```mermaid
flowchart TB
    subgraph ON_DISK["On disk"]
        direction TB
        REPO["📂 ExcelIngestion/<br/>Repository (code)"]
        EDATA["📂 ExcelIngestion_Data/<br/>DATA_ROOT (persistent data)"]
    end

    subgraph INPUT["📁 RAW INPUT LAYER"]
        direction LR
        TASKS_XLSX["📊 Tasks Excel Files<br/>Multiple .xlsx files<br/>~4.7M rows total"]
        WORKERS_XLSX["👥 Workers.xlsx<br/>~4K rows"]
        EMPLOYEES_XLSX["🏢 Employees_Master.xlsx<br/>~3K rows"]
        DEPT_XLSX["🗂️ Dept_Mapping.xlsx<br/>~200 rows"]
        REVENUE_XLSX["💰 Revenue.xlsx<br/>Revenue data"]
        LAUNCH_XLSX["🚀 Launch.xlsx<br/>Onboarding data"]
    end

    subgraph STEP01["Step 01: Discover Files"]
        DISCOVER["🔍 discover_files()<br/>Scan ExcelIngestion_Data/{env}/**/raw/"]
    end

    subgraph STEP02["Step 02: Validate Schema"]
        VALIDATE["✅ validate_schema()<br/>Check required columns<br/>Apply column_aliases"]
    end

    subgraph STEP03["Step 03: Deduplicate"]
        DEDUP["🔄 deduplicate()<br/>Remove exact duplicates<br/>Track _source_file"]
    end

    subgraph STEP04["Step 04: Normalize"]
        NORMALIZE["📐 normalize()<br/>Type casting<br/>Date parsing<br/>String cleaning"]
    end

    subgraph STEP05["Step 05: Combine"]
        COMBINE["📦 combine_files()<br/>Union all source files<br/>→ combined.parquet"]
    end

    subgraph PARQUET_LAYER["📁 PARQUET STAGING LAYER (under DATA_ROOT)"]
        direction LR
        TASKS_PQ["ExcelIngestion_Data/.../tasks/<br/>analytics/combined.parquet"]
        WORKERS_PQ["ExcelIngestion_Data/.../workers/<br/>analytics/combined.parquet"]
        EMPLOYEES_PQ["ExcelIngestion_Data/.../employees_master/<br/>analytics/combined.parquet"]
        DEPT_PQ["ExcelIngestion_Data/.../dept_mapping/<br/>analytics/combined.parquet"]
        REVENUE_PQ["ExcelIngestion_Data/.../revenue/<br/>analytics/combined.parquet"]
        LAUNCH_PQ["ExcelIngestion_Data/.../launch/<br/>analytics/combined.parquet"]
    end

    subgraph STEP06["Step 06: Validate Data"]
        DATA_VAL["🔍 validate_data()<br/>Null checks<br/>Type validation<br/>Range checks"]
    end

    subgraph STEP07["Step 07: Build Analytics"]
        ANALYTICS["📊 build_analytics()<br/>Aggregate metrics<br/>Summary stats"]
    end

    subgraph STEP08["Step 08: Export DuckDB"]
        DUCKDB_EXPORT["🦆 export_duckdb()<br/>→ ExcelIngestion_Data/powerbi/*.duckdb"]
    end

    subgraph STEP09["Step 09: Export SQLite"]
        SQLITE_EXPORT["🗃️ export_sqlite()<br/>→ ExcelIngestion_Data/analytics/{env}_warehouse.db<br/>Create base tables + indexes"]
    end

    subgraph SQLITE_DB["🗃️ SQLite Warehouse (Base Tables Only)"]
        direction TB

        subgraph BASE_TABLES["Base Tables"]
            T_TASKS["tasks<br/>4.7M rows"]
            T_WORKERS["workers<br/>~4K rows"]
            T_EMPLOYEES["employees_master<br/>~3K rows"]
            T_DEPT["employees (dept_mapping)"]
            T_REVENUE["revenue"]
            T_LAUNCH["launch"]
        end
    end

    subgraph DUCKDB_DB["🦆 DuckDB Warehouse"]
        direction TB
        DBT_MODELS["dbt_crc models<br/>DuckDB dialect SQL<br/>Source of truth"]
    end

    subgraph OUTPUT["📊 OUTPUT LAYER"]
        POWERBI["📈 Power BI<br/>Connects to SQLite<br/>or DuckDB"]
    end

    %% Flow connections
    TASKS_XLSX --> STEP01
    WORKERS_XLSX --> STEP01
    EMPLOYEES_XLSX --> STEP01
    DEPT_XLSX --> STEP01
    REVENUE_XLSX --> STEP01
    LAUNCH_XLSX --> STEP01

    STEP01 --> STEP02 --> STEP03 --> STEP04 --> STEP05

    STEP05 --> TASKS_PQ
    STEP05 --> WORKERS_PQ
    STEP05 --> EMPLOYEES_PQ
    STEP05 --> DEPT_PQ
    STEP05 --> REVENUE_PQ
    STEP05 --> LAUNCH_PQ

    PARQUET_LAYER --> STEP06 --> STEP07

    STEP07 --> STEP08
    STEP07 --> STEP09

    STEP08 --> DUCKDB_DB
    STEP09 --> SQLITE_DB

    DUCKDB_DB --> POWERBI
```

## Dataset Processing Flow

```mermaid
flowchart TB
    subgraph TASKS_FLOW["Tasks Dataset Flow"]
        T1["Raw Excel Files<br/>(multiple sources)"]
        T2["combined.parquet<br/>4.7M rows"]
        T3["SQLite: tasks table<br/>(base table)"]
        T4["DuckDB: stg_tasks → marts<br/>(via dbt)"]

        T1 -->|"Steps 01-05"| T2 -->|"Step 09"| T3
        T2 -->|"dbt run"| T4
    end

    subgraph WORKERS_FLOW["Workers Dataset Flow"]
        W1["Workers.xlsx"]
        W2["combined.parquet<br/>~4K rows"]
        W3["SQLite: workers table<br/>(base table)"]
        W4["DuckDB: stg_workers → marts<br/>(via dbt)"]

        W1 -->|"Steps 01-05"| W2 -->|"Step 09"| W3
        W2 -->|"dbt run"| W4
    end

    subgraph EMPLOYEES_FLOW["Employees Master Flow"]
        E1["Employees_Master.xlsx"]
        E2["combined.parquet<br/>~3K rows"]
        E3["SQLite: employees_master table<br/>(base table)"]
        E4["DuckDB: stg_employees → marts<br/>(via dbt)"]

        E1 -->|"Steps 01-05"| E2 -->|"Step 09"| E3
        E2 -->|"dbt run"| E4
    end
```

## File System Structure

```mermaid
flowchart TB
    subgraph REPO["ExcelIngestion/ (repository)"]
        direction TB

        subgraph DATASETS["datasets/ (templates for init)"]
            DEV["dev/"]
            PROD["prod/"]
        end

        subgraph LIB["lib/"]
            L1["Pipeline libraries"]
        end

        subgraph DBT["dbt_crc/"]
            DBT_SEEDS["seeds/"]
            DBT_STAGING["models/staging/"]
            DBT_MARTS["models/marts/"]
        end
    end

    subgraph EROOT["ExcelIngestion_Data/ (DATA_ROOT — persistent)"]
        direction TB

        subgraph EDEV["dev/ …"]
            D_TASKS["tasks/raw/*.xlsx<br/>tasks/analytics/combined.parquet"]
            D_WORKERS["workers/…"]
            D_EMPLOYEES["employees_master/…"]
            D_DEPT["dept_mapping/…"]
            D_REVENUE["revenue/…"]
            D_LAUNCH["launch/…"]
        end

        subgraph EANALYTICS["analytics/"]
            A_SQLITE["dev_warehouse.db<br/>warehouse.db"]
        end

        subgraph EPOWERBI["powerbi/"]
            P_DUCKDB["dev_warehouse.duckdb<br/>warehouse.duckdb"]
        end
    end
```

## Pipeline Execution Order

```mermaid
sequenceDiagram
    participant User
    participant Pipeline as run_pipeline.py
    participant Steps as Steps 01-09
    participant SQLite as SQLite DB
    participant dbt as dbt
    participant DuckDB as DuckDB

    User->>Pipeline: python run_pipeline.py --dataset tasks

    loop For each dataset
        Pipeline->>Steps: Step 01: Convert Excel → Parquet
        Pipeline->>Steps: Step 02: Normalize schema
        Pipeline->>Steps: Step 03-05: Clean, normalize values
        Pipeline->>Steps: Step 06: Combine → combined.parquet
        Pipeline->>Steps: Step 07-08: Handle nulls, validate
        Pipeline->>Steps: Step 09: Export to SQLite
        Steps->>SQLite: Write base tables + indexes
    end

    Pipeline->>User: Pipeline complete!

    Note over User,DuckDB: Then run dbt separately

    User->>dbt: dbt run
    dbt->>DuckDB: Load seeds
    dbt->>DuckDB: Create stg_* views
    dbt->>DuckDB: Create mart_* tables
    dbt->>User: dbt complete!
```

---

## dbt Model Lineage (DAG)

This shows the dependency graph of your dbt models - what `{{ ref() }}` and `{{ source() }}` create:

```mermaid
flowchart TB
    subgraph SOURCES["Sources (raw Parquet files)"]
        direction LR
        S_TASKS["source('raw', 'tasks')<br/>tasks/analytics/combined.parquet"]
        S_WORKERS["source('raw', 'workers')<br/>workers/analytics/combined.parquet"]
        S_EMPLOYEES["source('raw', 'employees_master')<br/>employees_master/analytics/combined.parquet"]
    end

    subgraph SEEDS["Seeds (reference CSVs)"]
        direction LR
        SEED_STATUS["value_map_taskstatus"]
        SEED_FLOW["value_map_flowname"]
    end

    subgraph STAGING["Staging Layer (views)"]
        direction LR
        STG_TASKS["stg_tasks<br/>(VIEW)<br/>Normalizes + applies value maps"]
        STG_WORKERS["stg_workers<br/>(VIEW)<br/>Cleans worker data"]
        STG_EMPLOYEES["stg_employees_master<br/>(VIEW)<br/>Dedupes by employee_id"]
    end

    subgraph MARTS["Marts Layer (tables)"]
        direction TB

        subgraph TASK_MARTS["Task-Based Marts"]
            M_ENRICHED["mart_tasks_enriched<br/>Full task + worker + employee join"]
            M_DEMAND["mart_team_demand<br/>Task volume by dept/week"]
            M_OFFSHORE["mart_onshore_offshore<br/>Tasks by source system"]
            M_BACKLOG["mart_backlog<br/>Open tasks by status"]
            M_TURNAROUND["mart_turnaround<br/>Completed task metrics"]
            M_TREND["mart_daily_trend<br/>Daily open/closed"]
        end

        subgraph WORKER_MARTS["Worker-Based Marts"]
            M_CAPACITY["mart_team_capacity<br/>Headcount by dept"]
        end
    end

    %% Source to Staging
    S_TASKS --> STG_TASKS
    S_WORKERS --> STG_WORKERS
    S_EMPLOYEES --> STG_EMPLOYEES
    SEED_STATUS --> STG_TASKS
    SEED_FLOW --> STG_TASKS

    %% Staging to Marts
    STG_TASKS --> M_ENRICHED
    STG_WORKERS --> M_ENRICHED
    STG_EMPLOYEES --> M_ENRICHED

    STG_TASKS --> M_DEMAND
    STG_WORKERS --> M_DEMAND

    STG_TASKS --> M_OFFSHORE
    STG_EMPLOYEES --> M_OFFSHORE

    STG_TASKS --> M_BACKLOG
    STG_TASKS --> M_TURNAROUND
    STG_TASKS --> M_TREND

    STG_WORKERS --> M_CAPACITY
```

## dbt Execution Flow

```mermaid
sequenceDiagram
    participant User
    participant dbt as dbt CLI
    participant DuckDB as DuckDB Warehouse

    User->>dbt: cd dbt_crc && dbt run

    Note over dbt: Phase 1: Seeds
    dbt->>DuckDB: Load value_map_taskstatus.csv
    dbt->>DuckDB: Load value_map_flowname.csv

    Note over dbt: Phase 2: Staging (views)
    dbt->>DuckDB: CREATE VIEW stg_tasks AS ...
    dbt->>DuckDB: CREATE VIEW stg_workers AS ...
    dbt->>DuckDB: CREATE VIEW stg_employees_master AS ...

    Note over dbt: Phase 3: Marts (tables)
    dbt->>DuckDB: CREATE TABLE mart_tasks_enriched AS ...
    dbt->>DuckDB: CREATE TABLE mart_team_capacity AS ...
    dbt->>DuckDB: CREATE TABLE mart_team_demand AS ...
    dbt->>DuckDB: CREATE TABLE mart_onshore_offshore AS ...
    dbt->>DuckDB: CREATE TABLE mart_backlog AS ...
    dbt->>DuckDB: CREATE TABLE mart_turnaround AS ...
    dbt->>DuckDB: CREATE TABLE mart_daily_trend AS ...

    dbt->>User: Completed successfully (7 models)
```

## dbt Model Details

| Model | Type | Dependencies | Description |
|-------|------|--------------|-------------|
| **stg_tasks** | view | source.tasks, seeds | Normalize assignedto, apply value maps |
| **stg_workers** | view | source.workers | Clean emails, pass through columns |
| **stg_employees_master** | view | source.employees_master | Dedupe by employee_id (latest wins) |
| **mart_tasks_enriched** | table | stg_tasks, stg_workers, stg_employees_master | Full denormalized task fact |
| **mart_team_capacity** | table | stg_workers | Headcount/FTE by cost center |
| **mart_team_demand** | table | stg_tasks, stg_workers | Task volume by department |
| **mart_onshore_offshore** | table | stg_tasks, stg_employees_master | Tasks by source_system |
| **mart_backlog** | table | stg_tasks | Open tasks aging |
| **mart_turnaround** | table | stg_tasks | Completed task handle time |
| **mart_daily_trend** | table | stg_tasks | Daily open/closed trend |

## How to Run dbt

**Important**: dbt requires Python 3.12, not the main venv's Python 3.14. Use the separate `.venv-dbt` environment.

```bash
# Activate dbt venv first (NOT the main .venv)
.venv-dbt\Scripts\activate

# Navigate to dbt project
cd "c:\Users\quang\CRC Code\ExcelIngestion\dbt_crc"

# First time only: install dependencies
dbt deps

# Run all models (seeds -> staging -> marts)
dbt run

# Or run specific parts:
dbt seed                      # Load CSVs only
dbt run --select staging      # Staging views only
dbt run --select marts        # Mart tables only
dbt run --select +mart_tasks_enriched  # mart + all upstream deps

# Generate and view documentation (interactive DAG!)
dbt docs generate
dbt docs serve    # Opens browser at localhost:8080
```

### Two Virtual Environments

| venv | Python | Purpose |
|------|--------|---------|
| `.venv` | 3.14 | Main pipeline (`run_pipeline.py`) |
| `.venv-dbt` | 3.12 | dbt models (`dbt run`, `dbt test`) |

## Tools to Visualize dbt

| Tool | How to Use | Best For |
|------|------------|----------|
| **dbt docs serve** | `dbt docs generate && dbt docs serve` | Official interactive DAG |
| **VS Code: dbt Power User** | Install from marketplace | In-editor navigation |
| **VS Code: vscode-dbt** | Install from marketplace | Syntax highlighting |
| **dbt Cloud** | cloud.getdbt.com | CI/CD + visual IDE |
| **Elementary** | `pip install elementary-data` | Data observability |
| **Mermaid in Markdown** | This file! | Quick static diagrams |

---

## How to View This Diagram

1. **VS Code**: Install the "Markdown Preview Mermaid Support" extension
2. **GitHub**: Renders automatically in .md files
3. **Online**: Paste into [mermaid.live](https://mermaid.live)

## Architecture Summary

| Layer | Location | Contents |
|-------|----------|----------|
| Pipeline (01-09) | Python | Excel → Parquet → SQLite (base tables) |
| dbt | DuckDB | stg_* views + mart_* tables |
| Power BI | DuckDB | Reads from dbt marts |

SQLite contains only base tables. All staging views and marts are in DuckDB via dbt.
