# employees_master Dataset — Step-by-Step Build Prompts

> **Dataset**: employees_master
> **Challenge**: 3 Excel files with DIFFERENT schemas must be unified
> **Source Files**: Brokerage.xlsx (20 rows), Select.xlsx (20 rows), Genpact.xlsx (20 rows)

---

## Pre-Build: Read These First

Before ANY prompt, start a new session with:

```
Read these files in order and confirm you understand the architecture:
1. current_state.md
2. sop.md
3. current_workflow.md

Then confirm: "Ready to build employees_master dataset."
```

---

## Prompt 1: Assessment Phase

```
# Assessment: employees_master Dataset

Read these files FIRST:
- current_state.md
- sop.md
- current_workflow.md
- lib/normalize_schema.py
- lib/schema.py
- lib/convert.py

Then examine the source files:
- new_datasets/Brokerage.xlsx
- new_datasets/Select.xlsx
- new_datasets/Genpact.xlsx

Report findings for each question:

1. **Folder Check**: Does datasets/dev/employees_master/ exist? What's in it?

2. **Schema Mapping Challenge**: The 3 source files have DIFFERENT column names:
   - "2025-2026 Hire" (Brokerage) vs "2025-26 Hire" (Select) → both should become "is_recent_hire"
   - "Brokerage Production team" vs "Office" → both should become "team_or_office"
   - "CRC Employee ID (Workday ID)" → should become "employee_id"

   Can the current normalize_schema.py handle this? It currently only does snake_case conversion.
   What's the cleanest way to add column aliasing without breaking existing datasets?

3. **Different Column Counts**: Brokerage/Select have 15 columns, Genpact has 7.
   Can the current pipeline handle combining files with different schemas?
   Step 03 (add_missing_columns) should handle this — confirm.

4. **source_system Column**: Each row needs to know its origin (Brokerage/Select/Genpact).
   _source_file already captures the filename. Options:
   a) Derive source_system from _source_file in a later step
   b) Add source_system during convert (step 01)
   c) Add source_system during combine (step 06)

   Which is cleanest?

5. **Blockers**: List any blockers or required code changes.

6. **Proposed Plan**: Based on findings, propose the implementation approach.

DO NOT write any code yet. Just report findings.
```

### Validation for Prompt 1:
- Assessment report is complete
- All 6 questions answered
- Clear recommendation on column aliasing approach
- No code changes made yet

---

## Prompt 2: Create Folder Structure

```
# Step 2: Create Folder Structure for employees_master

Following SOP Section 1.1 exactly, create the folder structure:

datasets/dev/employees_master/
├── raw/
├── clean/
├── errors/
├── analytics/
├── logs/
└── config/

datasets/prod/employees_master/
├── raw/
├── clean/
├── errors/
├── analytics/
├── logs/
└── config/

Use mkdir commands. Verify each directory exists after creation.

VALIDATION: Run `dir /B datasets\dev\employees_master` and `dir /B datasets\prod\employees_master` and show output.
```

### Validation for Prompt 2:
```bash
dir /B datasets\dev\employees_master
# Expected: analytics, clean, config, errors, logs, raw

dir /B datasets\prod\employees_master
# Expected: same structure
```

---

## Prompt 3: Create schema.yaml with Column Aliases

```
# Step 3: Create schema.yaml for employees_master

This dataset requires COLUMN ALIASING because source files have different column names that map to the same unified column.

Read these for reference:
- datasets/dev/tasks/config/schema.yaml
- datasets/dev/dept_mapping/config/schema.yaml

Create datasets/dev/employees_master/config/schema.yaml with:

1. **column_aliases** section (NEW) — maps source column names to unified names:
   ```yaml
   column_aliases:
     # Brokerage/Select variations
     "employee_id": "employee_id"
     "Employee ID": "employee_id"
     "2025-2026 Hire": "is_recent_hire"
     "2025-26 Hire": "is_recent_hire"
     "2025-2026 Term": "is_recent_term"
     "2025-26 Term": "is_recent_term"
     "Brokerage Production team": "team_or_office"
     "Office": "team_or_office"
     "Addressable Population?": "addressable_population"
     "Combined Hierarchy (Terms and New Hires)": "combined_hierarchy"

     # Genpact variations
     "CRC Employee ID (Workday ID)": "employee_id"
     "CRC Email Address": "email"
     "Employee Name": "name"
     "CRC Name": "genpact_crc_name"
     "Genpact ID": "genpact_id"
     "Phase": "genpact_phase"
     "2026 Mapping?": "genpact_mapping"
   ```

2. **columns** section — the unified schema (21 business columns + 3 system columns):
   - employee_id (string, not null)
   - name (string, not null)
   - supervisory_organization (string, nullable)
   - job_profile (string, nullable)
   - cost_center (string, nullable)
   - hire_date (datetime64, nullable)
   - is_recent_hire (string, nullable)
   - location (string, nullable)
   - term_date (datetime64, nullable)
   - is_recent_term (string, nullable)
   - term_reason (string, nullable)
   - role_disposition (string, nullable)
   - combined_hierarchy (string, nullable)
   - team_or_office (string, nullable)
   - addressable_population (string, nullable)
   - email (string, nullable)
   - genpact_id (string, nullable)
   - genpact_phase (string, nullable)
   - genpact_crc_name (string, nullable)
   - genpact_mapping (string, nullable)
   - source_system (string, not null) — will be derived from _source_file
   - _source_file (string, nullable)
   - _ingested_at (datetime64, nullable)

3. **validation** section:
   - max_null_rate: 0.80 (Genpact rows have many nulls — that's expected)
   - max_duplicate_rate: 1.0 (duplicates allowed — same employee can appear in multiple files)
   - min_row_count: 1

4. **column_order** section — list all columns in order

VALIDATION:
- Read back the created file
- Confirm column_aliases section exists
- Confirm all 24 columns defined
- Confirm validation allows high null rate
```

### Validation for Prompt 3:
```bash
type datasets\dev\employees_master\config\schema.yaml
# Should show column_aliases section and 24 columns
```

---

## Prompt 4: Create value_maps.yaml and combine.yaml

```
# Step 4: Create value_maps.yaml and combine.yaml

## value_maps.yaml

Create datasets/dev/employees_master/config/value_maps.yaml:

For now, keep it minimal — we can add mappings later if needed:
```yaml
# Value normalization maps for employees_master
# Add mappings here if source data has inconsistent values

addressable_population:
  "Yes": "Yes"
  "yes": "Yes"
  "Y": "Yes"
  "No": "No"
  "no": "No"
  "N": "No"

is_recent_hire:
  "Yes": "Yes"
  "yes": "Yes"
  "No": "No"
  "no": "No"

is_recent_term:
  "Yes": "Yes"
  "yes": "Yes"
  "No": "No"
  "no": "No"
```

## combine.yaml

Create datasets/dev/employees_master/config/combine.yaml:

```yaml
primary_key: row_id
output: combined.parquet
```

VALIDATION:
- Show contents of both files
- Confirm combine.yaml has primary_key: row_id
```

### Validation for Prompt 4:
```bash
type datasets\dev\employees_master\config\value_maps.yaml
type datasets\dev\employees_master\config\combine.yaml
```

---

## Prompt 5: Create pipeline.yaml

```
# Step 5: Create pipeline.yaml for employees_master

Create datasets/dev/employees_master/pipeline.yaml:

```yaml
# Dataset: employees_master (dev)
# Environment path: datasets/dev/employees_master
#
# This dataset combines 3 Excel files with DIFFERENT schemas:
# - Brokerage.xlsx (15 columns)
# - Select.xlsx (15 columns)
# - Genpact.xlsx (7 columns)
#
# Column aliasing in schema.yaml maps different source column names
# to unified column names (e.g., "2025-2026 Hire" → "is_recent_hire")

name: employees_master
environment: dev
description: "Unified employee dimension from Brokerage, Select, and Genpact sources"

# Use all steps — column aliasing handled in step 02
steps:
  - 01_convert
  - 02_normalize_schema
  - 03_add_missing_columns
  - 04_clean_errors
  - 05_normalize_values
  - 06_combine_datasets
  - 07_handle_nulls
  - 08_validate

sqlite:
  database: warehouse.db
  table: employees_master
  indexes:
    - employee_id
    - source_system
    - email
```

Note: Skipping steps 09 and 10 for now (SQLite export) — we can add later.

VALIDATION:
- Show the created file
- Confirm steps list includes 01-08
- Confirm sqlite table is employees_master
```

### Validation for Prompt 5:
```bash
type datasets\dev\employees_master\pipeline.yaml
```

---

## Prompt 6: Modify normalize_schema.py to Support Column Aliases

```
# Step 6: Add Column Alias Support to normalize_schema.py

The current normalize_schema.py only converts to snake_case. We need it to also apply column aliases from schema.yaml.

Read lib/normalize_schema.py and lib/schema.py first.

Modify lib/schema.py to add:
```python
def get_column_aliases(schema: dict) -> dict[str, str]:
    """Return column alias mapping from schema, or empty dict if not defined."""
    return schema.get("column_aliases", {})
```

Modify lib/normalize_schema.py process_file() to:
1. Load column_aliases from schema
2. After snake_case conversion, check if the result matches any alias key
3. If yes, use the alias target instead

The logic should be:
```python
# Get aliases
aliases = get_column_aliases(schema)

# Build rename map: original -> final_name
rename_map = {}
for c in original_columns:
    snake = to_snake_case(c)
    # Check if original column name (before snake_case) has an alias
    if c in aliases:
        rename_map[c] = aliases[c]
    # Check if snake_case version has an alias
    elif snake in aliases:
        rename_map[c] = aliases[snake]
    else:
        rename_map[c] = snake
```

This ensures:
- "Employee ID" → alias says "employee_id" → "employee_id"
- "2025-2026 Hire" → alias says "is_recent_hire" → "is_recent_hire"
- "Name" → no alias → snake_case → "name"

IMPORTANT: This change must be BACKWARD COMPATIBLE. Existing datasets (tasks, dept_mapping) don't have column_aliases and must continue to work unchanged.

VALIDATION:
1. Run existing tests: pytest tests/test_pipeline.py -v
2. All tests must pass
3. Show the modified code sections
```

### Validation for Prompt 6:
```bash
pytest tests/test_pipeline.py -v
# All existing tests must pass
```

---

## Prompt 7: Add source_system Derivation

```
# Step 7: Handle source_system Column

The source_system column should identify which file each row came from:
- Brokerage.xlsx → "Brokerage"
- Select.xlsx → "Select"
- Genpact.xlsx → "Genpact"

Options:
A) Add source_system during step 01 (convert) based on filename
B) Derive from _source_file in step 06 (combine) or step 07

Choose Option A — it's cleaner to add it early.

Modify lib/convert.py to:
1. Extract the base filename without extension
2. Add a source_system column with that value

Example: If processing "Brokerage.xlsx", add column source_system = "Brokerage"

Read lib/convert.py first, then make minimal changes.

IMPORTANT: This must only happen if source_system is defined in the schema. Check schema columns before adding.

VALIDATION:
1. Run existing tests: pytest tests/test_pipeline.py -v
2. All tests must pass (tasks/dept_mapping don't have source_system in schema, so unchanged)
```

### Validation for Prompt 7:
```bash
pytest tests/test_pipeline.py -v
# All existing tests must pass
```

---

## Prompt 8: Copy Source Files and Run Pipeline

```
# Step 8: Copy Source Files and Run Pipeline

1. Copy the 3 Excel files to raw/:
   copy new_datasets\Brokerage.xlsx datasets\dev\employees_master\raw\
   copy new_datasets\Select.xlsx datasets\dev\employees_master\raw\
   copy new_datasets\Genpact.xlsx datasets\dev\employees_master\raw\

2. Verify files exist:
   dir datasets\dev\employees_master\raw\

3. Run the pipeline:
   python run_pipeline.py --dataset employees_master

4. If errors occur, read the log and fix:
   type datasets\dev\employees_master\logs\pipeline.log

VALIDATION:
- Pipeline completes without errors
- Show last 20 lines of pipeline.log
- Show contents of analytics/ folder
```

### Validation for Prompt 8:
```bash
dir datasets\dev\employees_master\analytics\
# Should show combined.parquet

type datasets\dev\employees_master\logs\pipeline.log
# Should show successful completion
```

---

## Prompt 9: Validate Output Data

```
# Step 9: Validate Output Data

Run validation checks on the combined output:

```python
import pandas as pd

# Load combined data
df = pd.read_parquet('datasets/dev/employees_master/analytics/combined.parquet')

print("=== Basic Stats ===")
print(f"Total rows: {len(df)}")
print(f"Total columns: {len(df.columns)}")
print(f"Columns: {list(df.columns)}")

print("\n=== Rows per Source ===")
print(df['source_system'].value_counts())

print("\n=== Duplicate Employee IDs ===")
dup_ids = df[df.duplicated(subset=['employee_id'], keep=False)]
print(f"Rows with duplicate employee_id: {len(dup_ids)}")
if len(dup_ids) > 0:
    print("\nSample duplicates:")
    print(dup_ids[['employee_id', 'name', 'source_system']].head(10))

print("\n=== Null Rates ===")
null_rates = (df.isnull().sum() / len(df) * 100).round(1)
print(null_rates.to_string())

print("\n=== Sample Rows ===")
print(df[['employee_id', 'name', 'source_system', 'email', 'genpact_id']].head(10))
```

Expected results:
- Total rows: 60 (20 + 20 + 20)
- source_system values: Brokerage (20), Select (20), Genpact (20)
- Columns: 24 (21 business + row_id + _source_file + _ingested_at)
- Genpact rows should have high null rates for columns like supervisory_organization, job_profile, etc.
- Duplicate employee_ids may exist (same person in multiple files)

VALIDATION:
- Row count matches expected (60)
- All 24 columns present
- source_system correctly populated
- Null rates are as expected
```

---

## Prompt 10: Copy Config to Prod and Final Tests

```
# Step 10: Copy to Prod and Run Final Tests

1. Copy config to prod:
   xcopy /E /I datasets\dev\employees_master\config datasets\prod\employees_master\config
   copy datasets\dev\employees_master\pipeline.yaml datasets\prod\employees_master\

   # Edit prod pipeline.yaml to set environment: prod

2. Run all existing tests:
   pytest tests/ -v

   All tests must pass — existing datasets unaffected.

3. Verify existing datasets still work:
   python run_pipeline.py --dataset tasks --dry-run
   python run_pipeline.py --dataset dept_mapping --dry-run

VALIDATION:
- All pytest tests pass
- tasks dry-run succeeds
- dept_mapping dry-run succeeds
- prod config files exist
```

---

## Full Review Prompt

```
# employees_master Dataset — Full Validation Review

Run ALL validation checks and report in table format.

## 1. Folder Structure
- [ ] datasets/dev/employees_master/ exists with all subdirs
- [ ] datasets/prod/employees_master/ exists with all subdirs
- [ ] Config files present: schema.yaml, value_maps.yaml, combine.yaml, pipeline.yaml

## 2. Schema Validation
- [ ] schema.yaml has column_aliases section
- [ ] schema.yaml has 24 columns defined
- [ ] column_aliases maps all source variations correctly

## 3. Pipeline Execution
- [ ] python run_pipeline.py --dataset employees_master completes without errors
- [ ] combined.parquet created in analytics/
- [ ] No errors in pipeline.log

## 4. Data Validation
Run and report:
```python
import pandas as pd
df = pd.read_parquet('datasets/dev/employees_master/analytics/combined.parquet')

# Report these
print(f"Row count: {len(df)} (expected: 60)")
print(f"Column count: {len(df.columns)} (expected: 24)")
print(f"source_system values: {df['source_system'].value_counts().to_dict()}")
print(f"employee_id not null: {df['employee_id'].notna().sum()}")
```

## 5. Column Mapping Validation
Verify these mappings worked:
- "2025-2026 Hire" → is_recent_hire
- "Brokerage Production team" → team_or_office
- "CRC Employee ID (Workday ID)" → employee_id
- "CRC Email Address" → email

## 6. Backward Compatibility
- [ ] pytest tests/ -v passes (all existing tests)
- [ ] python run_pipeline.py --dataset tasks --dry-run succeeds
- [ ] python run_pipeline.py --dataset dept_mapping --dry-run succeeds

## 7. dbt Integration (if applicable)
- [ ] dbt_crc/models/sources.yml updated for employees_master
- [ ] dbt run --select stg_employees_master succeeds
- [ ] dbt test --select stg_employees_master passes

## Final Report

| Check | Status | Details |
|-------|--------|---------|
| Folder structure | ✅/❌ | |
| Config files | ✅/❌ | |
| Pipeline execution | ✅/❌ | |
| Row count | ✅/❌ | Expected: 60 |
| Column count | ✅/❌ | Expected: 24 |
| source_system | ✅/❌ | 3 values |
| Column mapping | ✅/❌ | Aliases applied |
| Existing tests | ✅/❌ | All pass |
| tasks pipeline | ✅/❌ | Unaffected |
| dept_mapping pipeline | ✅/❌ | Unaffected |

## Success Criteria
ALL checks must show ✅ for the build to be considered complete.
```

---

## Quick Reference

### File Locations After Build

| File | Path |
|------|------|
| Schema | datasets/dev/employees_master/config/schema.yaml |
| Value maps | datasets/dev/employees_master/config/value_maps.yaml |
| Combine config | datasets/dev/employees_master/config/combine.yaml |
| Pipeline config | datasets/dev/employees_master/pipeline.yaml |
| Source files | datasets/dev/employees_master/raw/*.xlsx |
| Output | datasets/dev/employees_master/analytics/combined.parquet |
| Logs | datasets/dev/employees_master/logs/pipeline.log |

### Key Commands

```bash
# Run pipeline
python run_pipeline.py --dataset employees_master

# Check logs
type datasets\dev\employees_master\logs\pipeline.log

# Validate output
python -c "import pandas as pd; print(pd.read_parquet('datasets/dev/employees_master/analytics/combined.parquet').info())"

# Run all tests
pytest tests/ -v
```
