# Standard Operating Procedures (SOP)

> **Purpose**: Step-by-step procedures for common tasks in the ExcelIngestion pipeline.

**Runtime data root:** Day-to-day operations use **`DATA_ROOT`** (default `../ExcelIngestion_Data`). Treat paths below as **`{DATA_ROOT}/{env}/{dataset}/...`** after you run `python scripts/init_data_directory.py` and copy or migrate configs. The in-repo **`datasets/`** tree remains the **template** used by `init_data_directory.py` and for optional `--pipeline datasets/...` runs.

---

## Table of Contents

1. [Adding a New Dataset](#1-adding-a-new-dataset)
2. [Adding dbt Layers on Top of a Dataset](#2-adding-dbt-layers-on-top-of-a-dataset)
3. [Adding a New Mart Layer](#3-adding-a-new-mart-layer)
4. [Adding a New Column to Existing Dataset](#4-adding-a-new-column-to-existing-dataset)
5. [Refreshing Data (Daily Operations)](#5-refreshing-data-daily-operations)
6. [Deploying to Production](#6-deploying-to-production)
7. [Troubleshooting Pipeline Failures](#7-troubleshooting-pipeline-failures)
8. [Adding a Dataset with Column Aliasing (Multi-Schema Sources)](#8-adding-a-dataset-with-column-aliasing-multi-schema-sources)
9. [Setting Up dbt Environment](#9-setting-up-dbt-environment)
10. [Setting Up External Data Directory](#10-setting-up-external-data-directory)

---

## 1. Adding a New Dataset

### Prerequisites
- Know the Excel file structure (columns, types)
- Have sample data for testing

### Step 1.1: Create Folder Structure

```bash
# Create for both dev and prod
mkdir -p datasets/dev/{new_dataset}/raw
mkdir -p datasets/dev/{new_dataset}/clean
mkdir -p datasets/dev/{new_dataset}/errors
mkdir -p datasets/dev/{new_dataset}/analytics
mkdir -p datasets/dev/{new_dataset}/logs
mkdir -p datasets/dev/{new_dataset}/config

# Copy structure to prod
xcopy /E /I datasets\dev\{new_dataset} datasets\prod\{new_dataset}
```

### Step 1.2: Create pipeline.yaml

Create `datasets/dev/{new_dataset}/pipeline.yaml`:

```yaml
name: {new_dataset}
environment: dev
description: "Description of this dataset"

# Optional: limit steps (default runs all 9)
# Omit this section to run all steps
# Note: Pipeline stops at step 09. dbt handles all analytics in DuckDB.
steps:
  - 01_convert
  - 02_normalize_schema
  - 04_clean_errors
  - 05_normalize_values
  - 08_validate
  - 09_export_sqlite

sqlite:
  database: warehouse.db
  table: {table_name}
  indexes:
    - primary_key_column
    - frequently_queried_column
```

### Step 1.3: Create schema.yaml

Create `datasets/dev/{new_dataset}/config/schema.yaml`:

```yaml
columns:
  # Define each column
  column1:
    dtype: string       # string, int64, float64, datetime64, bool
    nullable: false
    primary_key: true   # Optional

  column2:
    dtype: int64
    nullable: true

  date_column:
    dtype: datetime64
    nullable: false

  # Always include these auto-added columns
  _source_file:
    dtype: string
    nullable: true

  _ingested_at:
    dtype: datetime64
    nullable: true

# Column order in output (must match columns above)
column_order:
  - column1
  - column2
  - date_column
  - _source_file
  - _ingested_at

# Validation rules
validation:
  max_null_rate: 0.30      # Fail if any column > 30% null
  max_duplicate_rate: 0.01 # Fail if > 1% duplicate PKs
  min_row_count: 1         # Fail if empty
```

### Step 1.4: Create value_maps.yaml

Create `datasets/dev/{new_dataset}/config/value_maps.yaml`:

```yaml
# Map messy source values to canonical values
# Format: column_name: { "source": "target" }

status_column:
  "active": "Active"
  "ACTIVE": "Active"
  "inactive": "Inactive"
  "INACTIVE": "Inactive"

# Empty file is valid if no mappings needed
```

### Step 1.5: Create combine.yaml

Create `datasets/dev/{new_dataset}/config/combine.yaml`:

```yaml
primary_key: row_id          # Surrogate key (auto-generated)
output: combined.parquet     # Output filename
```

### Step 1.6: Copy Config to Prod

```bash
xcopy /E /I datasets\dev\{new_dataset}\config datasets\prod\{new_dataset}\config
copy datasets\dev\{new_dataset}\pipeline.yaml datasets\prod\{new_dataset}\

# Edit prod pipeline.yaml - change environment: dev to environment: prod
```

### Step 1.7: Test with Sample Data

```bash
# Drop sample Excel files in raw/
copy sample_data.xlsx datasets\dev\{new_dataset}\raw\

# Run pipeline
python run_pipeline.py --dataset {new_dataset}

# Check logs
type datasets\dev\{new_dataset}\logs\pipeline.log

# Verify output
python -c "import pandas as pd; df = pd.read_parquet('datasets/dev/{new_dataset}/analytics/combined.parquet'); print(df.info()); print(df.head())"
```

### Step 1.8: Create Test Fixtures (Optional)

Create `tests/create_{new_dataset}_fixtures.py` following the pattern in `create_fixtures.py`.

---

## 2. Adding dbt Layers on Top of a Dataset

### Prerequisites
- Dataset already exists and pipeline runs successfully
- `combined.parquet` is generated

### Step 2.1: Add Source in sources.yml

Edit `dbt_crc/models/sources.yml`:

```yaml
version: 2
sources:
  - name: raw
    tables:
      # ... existing tables ...

      - name: {new_dataset}
        description: "Description of the dataset"
        meta:
          external_location: "{{ env_var('DATA_ROOT', '../../ExcelIngestion_Data') }}/{{ env_var('PIPELINE_ENV', 'dev') }}/{new_dataset}/analytics/combined.parquet"
```

### Step 2.2: Create Staging Model

Create `dbt_crc/models/staging/stg_{new_dataset}.sql`:

```sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', '{new_dataset}') }}
),

cleaned as (
    select
        -- Surrogate key
        row_id,

        -- Normalize join keys (lowercase, trim)
        lower(trim(join_column)) as join_column,

        -- Business columns
        column1,
        column2,

        -- Date handling
        date_column,

        -- Lineage columns
        _source_file,
        _ingested_at

    from source
)

select * from cleaned
```

### Step 2.3: Add Tests in staging.yml

Edit `dbt_crc/models/staging/staging.yml`:

```yaml
version: 2

models:
  # ... existing models ...

  - name: stg_{new_dataset}
    description: "Staged {new_dataset} data with normalized keys"
    columns:
      - name: row_id
        description: "Surrogate primary key"
        tests:
          - not_null
          - unique

      - name: join_column
        description: "Normalized join key"
        tests:
          - not_null
```

### Step 2.4: Build and Test

```bash
cd dbt_crc

# Compile to check syntax
dbt compile --select stg_{new_dataset}

# Build the model
dbt run --select stg_{new_dataset}

# Run tests
dbt test --select stg_{new_dataset}

# Verify data
dbt run-operation generate_model_yaml --args '{"model_names": ["stg_{new_dataset}"]}'
```

---

## 3. Adding a New Mart Layer

### Existing Marts (Reference)

| Mart | Description |
|------|-------------|
| `mart_tasks_enriched` | Tasks with worker fields, employee source, duration metrics |
| `mart_team_capacity` | Headcount and FTE by cost center hierarchy |
| `mart_team_demand` | Task volume by cost center and date (daily) |
| `mart_onshore_offshore` | Task metrics by employee source system |
| `mart_backlog` | Open tasks by drawer/flow/step with age |
| `mart_turnaround` | Completed-task handle/lifecycle hours |
| `mart_daily_trend` | Daily opened vs completed by drawer |

### When to Create a New Mart

- Need aggregated metrics (daily, weekly, by category)
- Need denormalized view for reporting
- Need cross-dataset joins
- Need computed columns for business logic

### Step 3.1: Create Mart SQL

Create `dbt_crc/models/marts/mart_{name}.sql`:

```sql
{{ config(materialized='view') }}

with base as (
    select * from {{ ref('mart_tasks_enriched') }}  -- or stg_* model
),

aggregated as (
    select
        -- Dimensions (GROUP BY columns)
        dimension_column,

        -- Counts
        count(*) as total_count,
        sum(case when status = 'Completed' then 1 else 0 end) as completed_count,

        -- Averages
        avg(duration_hours) as avg_duration_hours,

        -- Computed metrics
        round(100.0 * sum(case when status = 'Completed' then 1 else 0 end)
              / nullif(count(*), 0), 1) as completion_rate_pct

    from base
    where dimension_column is not null  -- Filter out nulls if needed
    group by dimension_column
)

select * from aggregated
order by total_count desc
```

### Step 3.2: Add to marts.yml

Edit `dbt_crc/models/marts/marts.yml`:

```yaml
version: 2

models:
  # ... existing models ...

  - name: mart_{name}
    description: "Business metrics aggregated by {dimension}"
    columns:
      - name: dimension_column
        description: "Grouping dimension"
        tests:
          - not_null
          - unique

      - name: total_count
        description: "Total record count"
        tests:
          - not_null
```

### Step 3.3: Build and Validate

```bash
cd dbt_crc

# Build
dbt run --select mart_{name}

# Test
dbt test --select mart_{name}

# Preview data
# (Query directly in DuckDB or use dbt show if available)
```

---

## 4. Adding a New Column to Existing Dataset

### Step 4.1: Update schema.yaml

Edit `datasets/dev/{dataset}/config/schema.yaml`:

```yaml
columns:
  # ... existing columns ...

  new_column:
    dtype: string        # or int64, float64, datetime64, bool
    nullable: true       # Set based on data
    # Optional: allowed_values, fill_strategy, max_null_rate
```

Update `column_order` to include the new column in desired position.

### Step 4.2: Update value_maps.yaml (if needed)

If the new column needs value normalization:

```yaml
new_column:
  "bad_value": "Good Value"
  "BAD_VALUE": "Good Value"
```

### Step 4.3: Copy Changes to Prod

```bash
copy datasets\dev\{dataset}\config\schema.yaml datasets\prod\{dataset}\config\
copy datasets\dev\{dataset}\config\value_maps.yaml datasets\prod\{dataset}\config\
```

### Step 4.4: Rerun Pipeline

```bash
python run_pipeline.py --dataset {dataset}
```

### Step 4.5: Update dbt Models (if applicable)

Add the new column to the relevant staging model in `dbt_crc/models/staging/stg_{dataset}.sql`.

```bash
cd dbt_crc && dbt run --select stg_{dataset}+
```

---

## 5. Refreshing Data (Daily Operations)

### Dev Environment

```bash
# 1. Drop new Excel files in raw/
copy new_data.xlsx datasets\dev\tasks\raw\

# 2. Run Python pipeline
python run_pipeline.py --dataset tasks
python run_pipeline.py --dataset dept_mapping

# 3. Rebuild dbt models
cd dbt_crc
dbt run
dbt test

# 4. Verify
python tests/compare_dbt_vs_pipeline.py
```

### Prod Environment

```bash
# 1. Set environment
set PIPELINE_ENV=prod

# 2. Drop files
copy new_data.xlsx datasets\prod\tasks\raw\

# 3. Run pipelines
python run_pipeline.py --env prod --dataset tasks
python run_pipeline.py --env prod --dataset dept_mapping

# 4. Rebuild dbt
cd dbt_crc
dbt run --target prod
dbt test --target prod
```

---

## 6. Deploying to Production

### Pre-Deployment Checklist

- [ ] All dev tests pass (`pytest tests/ -v`)
- [ ] dbt tests pass (`dbt test`)
- [ ] Comparison script passes (`python tests/compare_dbt_vs_pipeline.py`)
- [ ] Config files copied to prod
- [ ] Sample data validated in prod

### Deployment Steps

```bash
# 1. Copy config files
xcopy /E /Y datasets\dev\{dataset}\config datasets\prod\{dataset}\config

# 2. Copy pipeline.yaml and update environment
copy datasets\dev\{dataset}\pipeline.yaml datasets\prod\{dataset}\
# Edit to set environment: prod

# 3. Test with prod data
set PIPELINE_ENV=prod
python run_pipeline.py --env prod --dataset {dataset} --dry-run

# 4. Full run
python run_pipeline.py --env prod --dataset {dataset}

# 5. Rebuild dbt for prod
cd dbt_crc
dbt run --target prod
dbt test --target prod
```

---

## 7. Troubleshooting Pipeline Failures

### Check Logs First

```bash
type datasets\dev\{dataset}\logs\pipeline.log
```

### Common Issues

| Error | Cause | Solution |
|-------|-------|----------|
| "Column not found" | Schema mismatch | Check schema.yaml column names match Excel |
| "Type cast failed" | Bad data | Check errors/ folder for bad rows |
| "Validation failed: null rate" | Too many nulls | Adjust max_null_rate or fix source data |
| "No files found" | Empty raw/ folder | Add Excel files to raw/ |
| dbt source error | Parquet missing | Run Python pipeline first |

### Debug Mode

```bash
# Run single step
python scripts/01_convert.py

# Check intermediate files
python -c "import pandas as pd; print(pd.read_parquet('datasets/dev/tasks/clean/file_batch_01.parquet').info())"

# Check error rows
python -c "import pandas as pd; print(pd.read_parquet('datasets/dev/tasks/errors/file_batch_01_errors.parquet'))"
```

### Reset and Rerun

```bash
# Clear intermediate files
del /Q datasets\dev\{dataset}\clean\*
del /Q datasets\dev\{dataset}\analytics\*
del /Q datasets\dev\{dataset}\errors\*

# Rerun from start
python run_pipeline.py --dataset {dataset}
```

---

## 8. Adding a Dataset with Column Aliasing (Multi-Schema Sources)

### When to Use

Use column aliasing when combining Excel files that have **different column headers** but represent the same logical data. For example:

- **employees_master**: Combines Brokerage.xlsx (15 columns), Select.xlsx (15 columns), and Genpact.xlsx (7 columns) into one unified schema.

### Step 8.1: Identify Source Columns

For each source file, list all column headers:

```bash
python -c "import pandas as pd; print(pd.read_excel('path/to/file.xlsx').columns.tolist())"
```

### Step 8.2: Design Unified Schema

Create a canonical column name for each logical field. Map all source variations to the canonical name.

**Example mapping table:**

| Canonical Name | Brokerage.xlsx | Select.xlsx | Genpact.xlsx |
|----------------|----------------|-------------|--------------|
| `employee_id` | "Employee ID" | "Employee ID" | "CRC Employee ID (Workday ID)" |
| `name` | "Name" | "Name" | "Employee Name" |
| `is_recent_hire` | "2025-2026 Hire" | "2025-26 Hire" | (not present) |

### Step 8.3: Create schema.yaml with column_aliases

```yaml
# datasets/dev/{dataset}/config/schema.yaml

column_aliases:
  # Map source column names → canonical names
  "Employee ID": "employee_id"
  "CRC Employee ID (Workday ID)": "employee_id"
  "Name": "name"
  "Employee Name": "name"
  "2025-2026 Hire": "is_recent_hire"
  "2025-26 Hire": "is_recent_hire"
  # ... all variations

columns:
  employee_id:
    dtype: string
    nullable: false
  name:
    dtype: string
    nullable: false
  is_recent_hire:
    dtype: string
    nullable: true
  # Source-specific columns (null when not present in source)
  genpact_id:
    dtype: string
    nullable: true
  # Lineage columns
  source_system:
    dtype: string
    nullable: false
  _source_file:
    dtype: string
    nullable: true
  _ingested_at:
    dtype: datetime64
    nullable: true

column_order:
  - employee_id
  - name
  - is_recent_hire
  - genpact_id
  - source_system
  - _source_file
  - _ingested_at

validation:
  # High null rate expected when columns are source-specific
  max_null_rate: 0.95
  max_duplicate_rate: 1.0
  min_row_count: 1
```

### Step 8.4: Add source_system Column

To track which source each row came from, derive `source_system` from `_source_file` in step 06 (combine). The pipeline automatically extracts the basename (e.g., "Brokerage", "Select", "Genpact").

### Step 8.5: Test with Sample Data

```bash
# Place all source Excel files in raw/
copy Brokerage.xlsx datasets\dev\{dataset}\raw\
copy Select.xlsx datasets\dev\{dataset}\raw\
copy Genpact.xlsx datasets\dev\{dataset}\raw\

# Run pipeline
python run_pipeline.py --dataset {dataset}

# Verify output
python -c "
import pandas as pd
df = pd.read_parquet('datasets/dev/{dataset}/analytics/combined.parquet')
print('Row count:', len(df))
print('Columns:', list(df.columns))
print('Source systems:', df['source_system'].value_counts())
"
```

### Step 8.6: Validate Aliasing Worked

Check that columns were correctly mapped:

```bash
python -c "
import pandas as pd
df = pd.read_parquet('datasets/dev/{dataset}/analytics/combined.parquet')
print('employee_id nulls:', df['employee_id'].isna().sum())
print('Sample employee_ids:', df['employee_id'].head(10).tolist())
"
```

### Key Points

- **column_aliases** is applied in step 02 (normalize_schema) before any other processing.
- Source-specific columns (e.g., `genpact_id`) will be null for rows from other sources.
- Set `max_null_rate: 0.95` in validation to accommodate source-specific columns.
- The `source_system` column helps track row origin for debugging and analysis.

---

## Quick Reference

### File Locations

| What | Path |
|------|------|
| Pipeline config | `datasets/{env}/{dataset}/pipeline.yaml` |
| Schema definition | `datasets/{env}/{dataset}/config/schema.yaml` |
| Value mappings | `datasets/{env}/{dataset}/config/value_maps.yaml` |
| Input files | `datasets/{env}/{dataset}/raw/*.xlsx` |
| Output parquet | `datasets/{env}/{dataset}/analytics/combined.parquet` |
| Error rows | `datasets/{env}/{dataset}/errors/*.parquet` |
| Pipeline logs | `datasets/{env}/{dataset}/logs/pipeline.log` |
| dbt models | `dbt_crc/models/staging/` and `dbt_crc/models/marts/` |

### Key Commands

```bash
# Python pipeline (use .venv)
.venv\Scripts\activate
python run_pipeline.py --dataset {name}              # Run full pipeline
python run_pipeline.py --dataset {name} --from-step 6  # Start from step 6
python run_pipeline.py --dataset {name} --force        # Reprocess all files
python run_pipeline.py --dry-run                      # Validate only

# dbt (use .venv-dbt)
.venv-dbt\Scripts\activate
cd dbt_crc
dbt deps                         # Install packages (first time)
dbt run                          # Build all models
dbt run --select model_name      # Build specific model
dbt run --select model_name+     # Build model and downstream
dbt test                         # Run all tests
dbt debug                        # Check connection

# Validation
pytest tests/ -v
python tests/compare_dbt_vs_pipeline.py
```

---

## 9. Setting Up dbt Environment

dbt requires Python 3.10-3.12 and does NOT work with Python 3.14. A separate virtual environment is required.

### Step 9.1: Check Python Versions Available

```bash
py --list
```

You should see Python 3.12 listed. If not, install it from python.org.

### Step 9.2: Create dbt Virtual Environment

```bash
py -3.12 -m venv .venv-dbt
.venv-dbt\Scripts\activate
pip install --upgrade pip
pip install dbt-core dbt-duckdb
```

### Step 9.3: Verify Installation

```bash
.venv-dbt\Scripts\activate
python --version          # Should show 3.12.x
dbt --version             # Should show dbt-core and dbt-duckdb
```

### Step 9.4: Test dbt Connection

```bash
cd dbt_crc
dbt debug
```

Should show:
- `profiles.yml file [OK found and valid]`
- `dbt_project.yml file [OK found and valid]`
- `Connection test: [OK connection ok]`
- `All checks passed!`

### Step 9.5: Install dbt Packages

```bash
cd dbt_crc
dbt deps
```

### Step 9.6: Run dbt Models

```bash
dbt run      # Build all models
dbt test     # Run data tests
```

Expected output: `PASS=11` models, `PASS=64` tests, `ERROR=0`.

### Two Venvs, Two Purposes

| venv | Python | Purpose | Commands |
|------|--------|---------|----------|
| `.venv` | 3.14 | Main pipeline | `python run_pipeline.py` |
| `.venv-dbt` | 3.12 | dbt models | `dbt run`, `dbt test` |

**Important**:
- Do NOT install dbt in `.venv` — it will fail
- Always activate the correct venv before running commands
- Add `.venv-dbt/` to `.gitignore`

---

## 10. Setting Up External Data Directory

### When to Use

- First-time setup on a new machine
- After downloading a new zip release of the code (your data stays outside the repo)
- Pointing the pipeline at a custom disk location (shared drive, larger disk, etc.)

### Step 10.1: Initialize Data Directory

From the project root (with `.venv` activated if you use one):

```bash
python scripts/init_data_directory.py
```

This creates the default **`../ExcelIngestion_Data`** folder (sibling to `ExcelIngestion/`), copies YAML templates from `datasets/` in the repo, and creates `dev/` and `prod/` dataset trees plus `analytics/` and `powerbi/`.

### Step 10.2: (Optional) Custom Location

**Command Prompt:**

```bat
set DATA_ROOT=D:\CustomPath
python scripts/init_data_directory.py
```

**PowerShell:**

```powershell
$env:DATA_ROOT="D:\CustomPath"
python scripts/init_data_directory.py
```

Use the same `DATA_ROOT` when running `run_pipeline.py`, `powerbi/*.py`, and `dbt` (see `dbt_crc/profiles.yml`).

### Step 10.3: Migrate Existing Data

If you still have files under the in-repo **`datasets/`** folder (or legacy root **`analytics/`** / **`powerbi/`**):

```bash
python scripts/migrate_data.py --dry-run
python scripts/migrate_data.py
```

Review the dry-run output, then run without `--dry-run` to move files into `{DATA_ROOT}`.

### Folder Structure

| Folder | Contents |
|--------|----------|
| `{DATA_ROOT}/dev/{dataset}/raw/` | Input Excel files |
| `{DATA_ROOT}/dev/{dataset}/clean/` | Intermediate Parquet |
| `{DATA_ROOT}/dev/{dataset}/analytics/` | `combined.parquet` |
| `{DATA_ROOT}/dev/{dataset}/_state/` | Fingerprint state (`ingestion_state.json`) |
| `{DATA_ROOT}/analytics/` | SQLite databases (`dev_warehouse.db` / `warehouse.db`) |
| `{DATA_ROOT}/powerbi/` | DuckDB files for Power BI and dbt |

`prod/` mirrors `dev/` when using `--env prod`.
