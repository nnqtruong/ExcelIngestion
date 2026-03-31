# ExcelIngestion Pipeline - Data Flow Visualization

## Complete Pipeline Architecture

```mermaid
flowchart TB
    subgraph INPUT["📁 RAW INPUT LAYER"]
        direction LR
        TASKS_XLSX["📊 Tasks Excel Files<br/>Multiple .xlsx files<br/>~4.7M rows total"]
        WORKERS_XLSX["👥 Workers.xlsx<br/>~4K rows"]
        EMPLOYEES_XLSX["🏢 Employees_Master.xlsx<br/>~3K rows"]
        DEPT_XLSX["🗂️ Dept_Mapping.xlsx<br/>~200 rows"]
    end

    subgraph STEP01["Step 01: Discover Files"]
        DISCOVER["🔍 discover_files()<br/>Scan datasets/{env}/**/raw/"]
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

    subgraph PARQUET_LAYER["📁 PARQUET STAGING LAYER"]
        direction LR
        TASKS_PQ["tasks/<br/>combined.parquet<br/>4.7M rows"]
        WORKERS_PQ["workers/<br/>combined.parquet<br/>~4K rows"]
        EMPLOYEES_PQ["employees_master/<br/>combined.parquet<br/>~3K rows"]
        DEPT_PQ["dept_mapping/<br/>combined.parquet<br/>~200 rows"]
    end

    subgraph STEP06["Step 06: Validate Data"]
        DATA_VAL["🔍 validate_data()<br/>Null checks<br/>Type validation<br/>Range checks"]
    end

    subgraph STEP07["Step 07: Build Analytics"]
        ANALYTICS["📊 build_analytics()<br/>Aggregate metrics<br/>Summary stats"]
    end

    subgraph STEP08["Step 08: Export DuckDB"]
        DUCKDB_EXPORT["🦆 export_duckdb()<br/>→ dev_warehouse.duckdb"]
    end

    subgraph STEP09["Step 09: Export SQLite"]
        SQLITE_EXPORT["🗃️ export_sqlite()<br/>→ analytics/{env}_warehouse.db<br/>Create base indexes"]
    end

    subgraph STEP10["Step 10: SQLite Views"]
        direction TB
        SEEDS["1️⃣ Load Seeds<br/>value_map_taskstatus<br/>value_map_flowname"]
        STG_VIEWS["2️⃣ Create stg_tasks view<br/>Apply value maps<br/>Normalize columns"]
        MAT_STAGING["3️⃣ CREATE TABLE<br/>_stg_tasks_enriched<br/>ONE expensive JOIN"]
        INDEXES["4️⃣ Create Indexes<br/>task_date, drawer,<br/>taskstatus, flowname..."]
        MART_VIEWS["5️⃣ Create 7 Mart Views<br/>Read from _stg_tasks_enriched"]
        VERIFY["6️⃣ Verify Views<br/>COUNT(*) per view"]

        SEEDS --> STG_VIEWS --> MAT_STAGING --> INDEXES --> MART_VIEWS --> VERIFY
    end

    subgraph SQLITE_DB["🗃️ SQLite Warehouse"]
        direction TB

        subgraph BASE_TABLES["Base Tables"]
            T_TASKS["tasks<br/>4.7M rows"]
            T_WORKERS["workers<br/>~4K rows"]
            T_EMPLOYEES["employees_master<br/>~3K rows"]
            T_SEEDS["value_map_*<br/>seed tables"]
        end

        subgraph STAGING["Staging Layer"]
            V_STG_TASKS["stg_tasks<br/>(VIEW)"]
            TBL_STG_ENRICHED["_stg_tasks_enriched<br/>(MATERIALIZED TABLE)<br/>Pre-joined + indexed"]
        end

        subgraph MARTS["Mart Views (7 total)"]
            M1["mart_tasks_enriched<br/>SELECT * FROM _stg_tasks_enriched"]
            M2["mart_team_capacity<br/>GROUP BY on workers"]
            M3["mart_team_demand<br/>GROUP BY on _stg_tasks_enriched"]
            M4["mart_onshore_offshore<br/>GROUP BY on _stg_tasks_enriched"]
            M5["mart_backlog<br/>GROUP BY on _stg_tasks_enriched"]
            M6["mart_turnaround<br/>GROUP BY on _stg_tasks_enriched"]
            M7["mart_daily_trend<br/>GROUP BY on _stg_tasks_enriched"]
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

    STEP01 --> STEP02 --> STEP03 --> STEP04 --> STEP05

    STEP05 --> TASKS_PQ
    STEP05 --> WORKERS_PQ
    STEP05 --> EMPLOYEES_PQ
    STEP05 --> DEPT_PQ

    PARQUET_LAYER --> STEP06 --> STEP07

    STEP07 --> STEP08
    STEP07 --> STEP09

    STEP08 --> DUCKDB_DB
    STEP09 --> STEP10

    STEP10 --> SQLITE_DB

    T_TASKS --> V_STG_TASKS
    T_SEEDS --> V_STG_TASKS
    V_STG_TASKS --> TBL_STG_ENRICHED
    T_WORKERS --> TBL_STG_ENRICHED
    T_EMPLOYEES --> TBL_STG_ENRICHED

    TBL_STG_ENRICHED --> M1
    TBL_STG_ENRICHED --> M3
    TBL_STG_ENRICHED --> M4
    TBL_STG_ENRICHED --> M5
    TBL_STG_ENRICHED --> M6
    TBL_STG_ENRICHED --> M7
    T_WORKERS --> M2

    SQLITE_DB --> POWERBI
    DUCKDB_DB --> POWERBI
```

## Step 10: SQLite Materialized Staging Architecture (Detail)

```mermaid
flowchart LR
    subgraph BEFORE["❌ BEFORE (Slow - 10+ min)"]
        direction TB
        B_TASKS["stg_tasks<br/>4.7M rows"]
        B_WORKERS["workers<br/>4K rows"]
        B_EMPLOYEES["employees_master<br/>3K rows"]

        B_M1["mart_tasks_enriched<br/>JOIN all 3 tables"]
        B_M3["mart_team_demand<br/>JOIN tasks + workers"]
        B_M4["mart_onshore_offshore<br/>JOIN tasks + employees"]

        B_TASKS --> B_M1
        B_WORKERS --> B_M1
        B_EMPLOYEES --> B_M1

        B_TASKS --> B_M3
        B_WORKERS --> B_M3

        B_TASKS --> B_M4
        B_EMPLOYEES --> B_M4
    end

    subgraph AFTER["✅ AFTER (Fast - <30 sec)"]
        direction TB
        A_TASKS["stg_tasks"]
        A_WORKERS["workers"]
        A_EMPLOYEES["employees_master"]

        A_STAGING["_stg_tasks_enriched<br/>(MATERIALIZED TABLE)<br/>ONE JOIN, indexed"]

        A_M1["mart_tasks_enriched<br/>SELECT *"]
        A_M3["mart_team_demand<br/>GROUP BY"]
        A_M4["mart_onshore_offshore<br/>GROUP BY"]

        A_TASKS --> A_STAGING
        A_WORKERS --> A_STAGING
        A_EMPLOYEES --> A_STAGING

        A_STAGING --> A_M1
        A_STAGING --> A_M3
        A_STAGING --> A_M4
    end
```

## Dataset Processing Flow

```mermaid
flowchart TB
    subgraph TASKS_FLOW["Tasks Dataset Flow"]
        T1["Raw Excel Files<br/>(multiple sources)"]
        T2["combined.parquet<br/>4.7M rows"]
        T3["SQLite: tasks table"]
        T4["stg_tasks view<br/>(value maps applied)"]
        T5["_stg_tasks_enriched<br/>(joined with workers/employees)"]
        T6["7 Mart Views"]

        T1 -->|"Steps 01-05"| T2 -->|"Step 09"| T3 -->|"Step 10"| T4 --> T5 --> T6
    end

    subgraph WORKERS_FLOW["Workers Dataset Flow"]
        W1["Workers.xlsx"]
        W2["combined.parquet<br/>~4K rows"]
        W3["SQLite: workers table"]
        W4["Joined into<br/>_stg_tasks_enriched"]
        W5["mart_team_capacity<br/>(direct query)"]

        W1 -->|"Steps 01-05"| W2 -->|"Step 09"| W3 --> W4
        W3 --> W5
    end

    subgraph EMPLOYEES_FLOW["Employees Master Flow"]
        E1["Employees_Master.xlsx"]
        E2["combined.parquet<br/>~3K rows"]
        E3["SQLite: employees_master table"]
        E4["Joined into<br/>_stg_tasks_enriched"]

        E1 -->|"Steps 01-05"| E2 -->|"Step 09"| E3 --> E4
    end
```

## File System Structure

```mermaid
flowchart TB
    subgraph ROOT["ExcelIngestion/"]
        direction TB

        subgraph DATASETS["datasets/"]
            DEV["dev/"]
            PROD["prod/"]

            subgraph DEV_CONTENT["dev/ structure"]
                D_TASKS["tasks/<br/>├── raw/*.xlsx<br/>├── staging/*.parquet<br/>└── analytics/combined.parquet"]
                D_WORKERS["workers/<br/>├── raw/*.xlsx<br/>└── analytics/combined.parquet"]
                D_EMPLOYEES["employees_master/<br/>├── raw/*.xlsx<br/>└── analytics/combined.parquet"]
            end
        end

        subgraph ANALYTICS["analytics/"]
            A_SQLITE["dev_warehouse.db<br/>prod_warehouse.db"]
        end

        subgraph POWERBI["powerbi/"]
            P_DUCKDB["dev_warehouse.duckdb<br/>prod_warehouse.duckdb"]
        end

        subgraph LIB["lib/"]
            L1["discover.py<br/>validate_schema.py<br/>deduplicate.py<br/>normalize.py<br/>combine.py"]
            L2["validate_data.py<br/>build_analytics.py<br/>export_duckdb.py<br/>export_sqlite.py<br/>sqlite_views.py"]
        end

        subgraph DBT["dbt_crc/"]
            DBT_SEEDS["seeds/<br/>value_map_*.csv"]
            DBT_STAGING["models/staging/<br/>stg_*.sql"]
            DBT_MARTS["models/marts/<br/>mart_*.sql"]
        end
    end
```

## _stg_tasks_enriched Table Schema

```mermaid
erDiagram
    _stg_tasks_enriched {
        string row_id PK
        string taskid
        string drawer
        string policynumber
        string filename
        string filenumber
        date effectivedate
        string carrier
        string acctexec
        string taskdescription
        string assignedto FK
        string taskfrom
        string operationby
        string flowname
        string stepname
        string sentto
        datetime dateavailable
        datetime dateinitiated
        datetime dateended
        string taskstatus
        datetime starttime
        datetime endtime
        string _source_file
        datetime _ingested_at
        string teammate "from workers"
        string worker_job_profile "from workers"
        string business_title "from workers"
        string management_level "from workers"
        string cost_center "from workers"
        string cost_center_hierarchy "from workers"
        float fte "from workers"
        float scheduled_weekly_hours "from workers"
        string direct_manager "from workers"
        string worker_status "from workers"
        string employee_source "from employees_master"
        string employee_master_name "from employees_master"
        float duration_minutes "computed"
        float duration_hours "computed"
        float lifecycle_hours "computed"
        date task_date "computed"
    }

    workers {
        string employee_id PK
        string teammate
        string job_profile
        string business_title
        string management_level
        string cost_center
        string cost_center_hierarchy
        float fte
        float scheduled_weekly_hours
        string direct_manager
        string current_status
    }

    employees_master {
        string employee_id PK
        string name
        string source_system
    }

    stg_tasks {
        string row_id PK
        string assignedto
        string taskstatus
        string flowname
    }

    stg_tasks ||--o{ _stg_tasks_enriched : "base data"
    workers ||--o{ _stg_tasks_enriched : "LEFT JOIN on assignedto"
    employees_master ||--o{ _stg_tasks_enriched : "LEFT JOIN on assignedto"
```

## Mart Views Dependencies

```mermaid
flowchart TB
    subgraph SOURCE_TABLES["Source Tables"]
        WORKERS["workers"]
        STG_ENRICHED["_stg_tasks_enriched<br/>(4.7M rows, indexed)"]
    end

    subgraph MART_VIEWS["7 Mart Views"]
        M1["mart_tasks_enriched<br/>SELECT * (pass-through)"]
        M2["mart_team_capacity<br/>Headcount by dept/cost center"]
        M3["mart_team_demand<br/>Task volume by dept/week"]
        M4["mart_onshore_offshore<br/>Tasks by source system"]
        M5["mart_backlog<br/>Open tasks by drawer/flow"]
        M6["mart_turnaround<br/>Completed task metrics"]
        M7["mart_daily_trend<br/>Daily open/closed counts"]
    end

    STG_ENRICHED --> M1
    WORKERS --> M2
    STG_ENRICHED --> M3
    STG_ENRICHED --> M4
    STG_ENRICHED --> M5
    STG_ENRICHED --> M6
    STG_ENRICHED --> M7

    subgraph INDEXES["Indexes on _stg_tasks_enriched"]
        I1["idx_stg_task_date"]
        I2["idx_stg_drawer"]
        I3["idx_stg_taskstatus"]
        I4["idx_stg_flowname"]
        I5["idx_stg_cost_center"]
        I6["idx_stg_source"]
        I7["idx_stg_stepname"]
    end

    STG_ENRICHED --- INDEXES
```

## Pipeline Execution Order

```mermaid
sequenceDiagram
    participant User
    participant Pipeline as run_pipeline.py
    participant Steps as Steps 01-10
    participant SQLite as SQLite DB
    participant DuckDB as DuckDB

    User->>Pipeline: python run_pipeline.py --all

    loop For each dataset
        Pipeline->>Steps: Step 01: Discover files
        Pipeline->>Steps: Step 02: Validate schema
        Pipeline->>Steps: Step 03: Deduplicate
        Pipeline->>Steps: Step 04: Normalize
        Pipeline->>Steps: Step 05: Combine → combined.parquet
    end

    Pipeline->>Steps: Step 06: Validate data
    Pipeline->>Steps: Step 07: Build analytics

    par Export to both databases
        Pipeline->>Steps: Step 08: Export DuckDB
        Steps->>DuckDB: Write tables
        Pipeline->>Steps: Step 09: Export SQLite
        Steps->>SQLite: Write tables + indexes
    end

    Pipeline->>Steps: Step 10: SQLite Views
    Steps->>SQLite: Load seed tables
    Steps->>SQLite: Create stg_tasks view
    Steps->>SQLite: CREATE TABLE _stg_tasks_enriched (JOIN)
    Steps->>SQLite: CREATE INDEXES
    Steps->>SQLite: CREATE 7 mart views
    Steps->>SQLite: Verify with COUNT(*)

    Pipeline->>User: Pipeline complete!
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

## Key Performance Insight

The critical optimization is the **_stg_tasks_enriched** materialized table:

| Metric | Before | After |
|--------|--------|-------|
| Step 10 Duration | 10+ minutes | <30 seconds |
| JOIN Operations | 3+ (per view query) | 1 (at table creation) |
| View Query Speed | Slow (re-computes JOIN) | Fast (indexed scan) |

The expensive `tasks ⟕ workers ⟕ employees_master` JOIN runs **once** when creating `_stg_tasks_enriched`. All 7 mart views then read from this pre-joined, indexed table.
