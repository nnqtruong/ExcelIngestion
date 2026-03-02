# Excel Ingestion Pipeline

An 8-step data pipeline that converts Excel files into clean, validated Parquet datasets. Handles schema normalization, missing columns, type casting, value standardization, and combines multiple source files into a single analytics-ready output.

## Project Structure

```
ExcelIngestion/
├── raw/                    # Input Excel files (.xlsx) - drop source files here
├── clean/                  # Intermediate Parquet files after per-file processing
├── analytics/              # Final combined output (combined.parquet)
├── logs/                   # Pipeline logs and validation reports
│   ├── pipeline.log        # Timestamped log of all pipeline steps
│   └── validation_report.json  # Final validation results (pass/fail per check)
├── config/
│   ├── schema.yaml         # Target schema: column definitions, types, validation rules
│   ├── value_maps.yaml     # Value normalization mappings (fix casing, typos)
│   └── combine.yaml        # How to combine files (union/join, primary key)
├── scripts/
│   ├── 01_convert.py       # Excel → Parquet
│   ├── 02_normalize_schema.py  # Lowercase snake_case column names
│   ├── 03_add_missing_columns.py  # Add schema columns that are missing
│   ├── 04_clean_errors.py  # Cast types, flag bad rows to _errors.parquet
│   ├── 05_normalize_values.py  # Apply value_maps.yaml transformations
│   ├── 06_combine_datasets.py  # Union all files, validate primary key
│   ├── 07_handle_nulls.py  # Apply fill strategies from schema
│   └── 08_validate.py      # Final gate: row count, nulls, dtypes, duplicates
├── tests/
│   ├── test_pipeline.py    # Pytest test suite
│   ├── create_fixtures.py  # Generate 12 test Excel files
│   └── fixtures/           # Sample Excel files for testing
├── run_pipeline.py         # Orchestrator script (runs steps 01-08)
└── requirements.txt        # Python dependencies
```

## Quick Start

```bash
# Clone and navigate to project
cd ExcelIngestion

# Create virtual environment
python -m venv .venv

# Activate venv (Windows)
.venv\Scripts\activate

# Activate venv (macOS/Linux)
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Generate test fixtures (optional - creates 12 sample Excel files)
python tests/create_fixtures.py

# Run the full pipeline
python run_pipeline.py

# Check output
ls analytics/       # Should contain combined.parquet
ls logs/           # pipeline.log and validation_report.json
```

## Pipeline Steps

### Step 01: Convert (`01_convert.py`)
Reads all `.xlsx` and `.xls` files from `raw/` and converts them to Parquet format in `clean/`. Uses openpyxl for Excel reading and pyarrow for Parquet output. Each input file produces one output file with the same base name.

**Reads:** `raw/*.xlsx`, `raw/*.xls`
**Outputs:** `clean/{filename}.parquet`
**Logs:** File conversion count

### Step 02: Normalize Schema (`02_normalize_schema.py`)
Renames all columns to lowercase snake_case (e.g., `TaskID` → `taskid`, `Effective Date` → `effective_date`). Reorders columns to match the order defined in `schema.yaml`. Extra columns not in the schema are appended at the end.

**Reads:** `clean/*.parquet`, `config/schema.yaml`
**Outputs:** Overwrites `clean/*.parquet`
**Logs:** Per-file normalization

### Step 03: Add Missing Columns (`03_add_missing_columns.py`)
Adds any columns defined in `schema.yaml` that are missing from the data, with null values and the correct dtype. This ensures all files have a consistent column set even if source files are incomplete.

**Reads:** `clean/*.parquet`, `config/schema.yaml`
**Outputs:** Overwrites `clean/*.parquet`
**Logs:** Per-file column additions (e.g., "Added column taskstatus to tasks_batch_07.parquet")

### Step 04: Clean Errors (`04_clean_errors.py`)
Casts each column to its schema-defined dtype (int64, float64, datetime64, string, boolean). Rows that fail type casting are moved to a sidecar file `{filename}_errors.parquet` for inspection. The main file retains only clean rows.

**Reads:** `clean/*.parquet`, `config/schema.yaml`
**Outputs:** Overwrites `clean/*.parquet`, creates `clean/*_errors.parquet` if errors exist
**Logs:** Cast error counts per column

### Step 05: Normalize Values (`05_normalize_values.py`)
Applies value mappings from `value_maps.yaml` to standardize categorical values. Fixes issues like inconsistent casing (`"completed"` → `"Completed"`), typos, and formatting problems (double spaces in drawer names).

**Reads:** `clean/*.parquet`, `config/value_maps.yaml`
**Outputs:** Overwrites `clean/*.parquet`
**Logs:** Remap counts per column (e.g., "column flowname - 6 value(s) remapped")

### Step 06: Combine Datasets (`06_combine_datasets.py`)
Unions all cleaned Parquet files into a single combined dataset. Validates that the primary key (defined in `combine.yaml`) has no duplicates. Supports both `union` mode (concat all) and `join` mode (merge on key).

**Reads:** `clean/*.parquet` (excluding `*_errors.parquet`), `config/combine.yaml`
**Outputs:** `analytics/combined.parquet`
**Logs:** Row counts per file before/after combination

### Step 07: Handle Nulls (`07_handle_nulls.py`)
Applies null-filling strategies defined in `schema.yaml`. Supported strategies: `fill_zero`, `fill_forward`, `fill_backward`, `fill_unknown`. Only applies to columns with a `fill_strategy` specified.

**Reads:** `analytics/combined.parquet`, `config/schema.yaml`
**Outputs:** Overwrites `analytics/combined.parquet`
**Logs:** Null rate before/after per column

### Step 08: Validate (`08_validate.py`)
Final validation gate. Checks row count, required columns, duplicate rate on primary key, null rate per column, and dtype correctness. Writes a detailed JSON report. Pipeline fails if any threshold is breached.

**Reads:** `analytics/combined.parquet`, `config/schema.yaml`, `config/combine.yaml`
**Outputs:** `logs/validation_report.json`
**Logs:** Pass/fail per check, detailed failure reasons

## Configuration

### schema.yaml

Defines the target schema: column names, data types, nullability, and validation thresholds.

```yaml
columns:
  taskid:
    dtype: int64          # Supported: string, int64, float64, datetime64, bool
    nullable: false       # If false, column must exist and is a required column
    primary_key: true     # Used for duplicate detection

  taskstatus:
    dtype: string
    nullable: true
    max_null_rate: 0.60   # Per-column override (default is global max_null_rate)
    allowed_values:       # Optional: list of valid values for documentation
      - "Completed"
      - "In Progress"
      - "Pending"

  dateavailable:
    dtype: datetime64
    nullable: true
    fill_strategy: null   # Options: fill_zero, fill_forward, fill_backward, fill_unknown

validation:
  max_null_rate: 0.30     # Global threshold: fail if any column > 30% null
  max_duplicate_rate: 0.01  # Fail if > 1% duplicate primary keys
  min_row_count: 1        # Fail if empty after cleaning

column_order:             # Output column order
  - taskid
  - drawer
  # ... remaining columns
```

**When to update schema.yaml:**
- Adding a new column: Add entry under `columns` with dtype and nullable
- Changing a column type: Update the `dtype` value
- Adjusting validation: Modify thresholds in `validation` section
- Adding fill strategy: Add `fill_strategy` to column definition

### value_maps.yaml

Maps raw values to standardized values. Applied during step 05.

```yaml
taskstatus:
  "completed": "Completed"    # Fix lowercase
  "COMPLETED": "Completed"    # Fix uppercase
  "in progress": "In Progress"
  "inprogress": "In Progress" # Fix missing space

flowname:
  "uw renewal": "UW Renewal"

drawer:
  "SCU  Bothell": "SCU Bothell"  # Fix double space
```

**When to update value_maps.yaml:**
- New variant discovered in source data
- Typos or casing issues found in validation
- Adding mappings for a new categorical column

**Example: Adding a new value mapping**
```yaml
carrier:
  "Markel west": "Markel West"  # Fix casing
  "ZURICH": "Zurich North America"  # Standardize name
```

### combine.yaml

Controls how files are combined.

```yaml
mode: union         # "union" (concat) or "join" (merge)
primary_key:
  - taskid          # Column(s) that must be unique after combination
output: combined.parquet  # Output filename
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test class
pytest tests/test_pipeline.py::TestSchemaConfig -v

# Regenerate test fixtures (12 Excel files with various data quality issues)
python tests/create_fixtures.py

# Test fixtures include:
# - Files 01-06: Full 21 columns
# - Files 07-12: Missing TaskStatus column (20 columns)
# - Random nulls in AcctExec, filename, SentTo
# - Casing variations in flowname and TaskStatus
# - Double spaces in Drawer names
```

**Adding new test cases:**
1. Add test Excel files to `tests/fixtures/`
2. Create new test methods in `tests/test_pipeline.py`
3. Follow existing patterns: load fixtures, apply transformations, assert expectations

## Adding New Source Files

When a new batch of Excel files arrives:

1. Drop the `.xlsx` files into `raw/`
2. Run the pipeline: `python run_pipeline.py`
3. Check `logs/validation_report.json` for any issues
4. If new value variants appear, update `config/value_maps.yaml`
5. If new columns appear, update `config/schema.yaml`

The pipeline is idempotent—rerunning it will reprocess all files from scratch.

## Known Data Issues

**6 of 12 source files are missing the TaskStatus column.** The pipeline handles this by:
1. Step 03 adds the missing `taskstatus` column with null values
2. Schema allows `taskstatus` to be nullable with `max_null_rate: 0.60`
3. Combined output has ~53% null rate for `taskstatus` (expected for 6/12 files missing it)

If future source files include TaskStatus, the null rate will naturally decrease.

## Troubleshooting

### "ModuleNotFoundError: No module named 'pandas'"
Virtual environment not activated. Run:
```bash
.venv\Scripts\activate   # Windows
source .venv/bin/activate  # macOS/Linux
```

### "No Excel files in raw/"
No input files found. Add `.xlsx` files to the `raw/` directory.

### "Required columns missing in {file}.parquet"
Source file is missing columns that are marked `nullable: false` in schema.yaml. Either:
- Add the missing columns to source data
- Set `nullable: true` in schema.yaml if the column is truly optional

### "Validation failed: null_rate_per_column"
A column has more nulls than allowed. Options:
- Set per-column `max_null_rate` in schema.yaml
- Add a `fill_strategy` to fill nulls
- Fix the source data

### "Duplicate primary key(s) after combination"
Multiple rows have the same primary key value. Check:
- Are source files overlapping?
- Is the primary key column correct in combine.yaml?

### "Step X failed with exit code 1"
Check `logs/pipeline.log` for detailed error messages. Common causes:
- Schema mismatch between config and data
- Invalid YAML syntax in config files
- File permission issues

## Clean Teardown

To reset everything and start fresh:

```bash
# Windows (PowerShell)
deactivate
Remove-Item -Recurse -Force .venv, clean, analytics, report, logs

# Windows (cmd)
deactivate
rmdir /s /q .venv clean analytics report logs

# macOS/Linux
deactivate && rm -rf .venv clean/ analytics/ report/ logs/
```

Then follow Quick Start to recreate the environment.
