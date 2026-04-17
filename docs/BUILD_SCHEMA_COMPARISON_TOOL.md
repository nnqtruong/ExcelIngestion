# Schema Comparison Tool — Build Instructions

## Overview

Create a schema comparison utility that reads Excel headers from raw/ directories and reports differences. This tool helps catch schema drift before pipeline failures.

**Assignee**: Junior Developer
**Reviewer**: Senior Engineer
**Estimated Complexity**: Medium (single file, ~300 lines)

---

## Prerequisites

Before starting, confirm you understand:
- The pipeline has 9 steps (01-09), dbt handles analytics
- Data lives in external directory: `DATA_ROOT/dev/{dataset}/raw/`
- Default DATA_ROOT: `../ExcelIngestion_Data` (sibling to repo)
- Existing `scripts/diagnose_schema.py` compares Parquet files (post-conversion)
- New tool compares Excel headers (pre-conversion) — separate purpose

---

## Step 1: Create Basic Script Structure

**File**: `scripts/compare_schemas.py`

Create the script with:
- Imports: `argparse`, `sys`, `pathlib`, `json`, `openpyxl`, `yaml`
- Use existing `lib/data_root.py` for `get_data_root()` and `get_dataset_path()`
- Accept `--dataset` and `--env` flags (like run_pipeline.py)
- Header-only reads using openpyxl `read_only=True`, `max_row=1`

```python
"""Compare Excel headers across raw files to detect schema drift."""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.data_root import get_data_root, get_dataset_path
# ... rest of implementation
```

### Validation Checkpoint 1
```bash
.venv/Scripts/python.exe scripts/compare_schemas.py --help
```
**Expected**: Shows help with `--dataset`, `--env` options. No errors.

---

## Step 2: Implement Header Reading

Create function to read Excel headers only (fast, no data loading):

```python
def read_excel_headers(filepath: Path) -> list[str]:
    """Read only the header row from an Excel file. Returns list of column names."""
    from openpyxl import load_workbook
    wb = load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active
    headers = []
    for row in ws.iter_rows(min_row=1, max_row=1, values_only=True):
        headers = [str(cell).strip() if cell is not None else f"_unnamed_{i}"
                   for i, cell in enumerate(row)]
        break
    wb.close()
    return headers
```

### Validation Checkpoint 2
```bash
.venv/Scripts/python.exe -c "
from pathlib import Path
import sys
sys.path.insert(0, '.')
from scripts.compare_schemas import read_excel_headers
from lib.data_root import get_dataset_path

raw_dir = get_dataset_path('dev', 'tasks') / 'raw'
for f in raw_dir.glob('*.xlsx'):
    print(f'{f.name}: {len(read_excel_headers(f))} columns')
    break
"
```
**Expected**: Prints file name and column count. Completes in <1 second.

---

## Step 3: Implement Core Comparison Logic

Build the main comparison that computes:
- `union_columns`: All unique columns across all files
- `intersection_columns`: Columns present in EVERY file
- `per_file_columns`: Dict of {filename: [columns]}
- `extra_columns`: Columns not in all files → {col: [files_that_have_it]}
- `missing_columns`: Columns missing from some files → {col: [files_missing_it]}

```python
def compare_schemas(raw_dir: Path) -> dict:
    """Compare headers across all Excel files in directory."""
    files = sorted(raw_dir.glob("*.xlsx"))
    if not files:
        return {"error": "No Excel files found"}

    file_headers = {}
    for f in files:
        file_headers[f.name] = read_excel_headers(f)

    all_columns = set()
    for headers in file_headers.values():
        all_columns.update(headers)

    intersection = set(file_headers[files[0].name])
    for headers in file_headers.values():
        intersection &= set(headers)

    # Build extra/missing maps
    extra = {}
    missing = {}
    for col in all_columns:
        has_it = [fn for fn, hdrs in file_headers.items() if col in hdrs]
        missing_it = [fn for fn, hdrs in file_headers.items() if col not in hdrs]
        if len(has_it) < len(file_headers):
            extra[col] = has_it
        if missing_it:
            missing[col] = missing_it

    return {
        "directory": str(raw_dir),
        "file_count": len(files),
        "union_columns": sorted(all_columns),
        "intersection_columns": sorted(intersection),
        "files": {fn: {"columns": hdrs, "column_count": len(hdrs)}
                  for fn, hdrs in file_headers.items()},
        "extra_columns": extra,
        "missing_columns": missing,
    }
```

### Validation Checkpoint 3
```bash
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset tasks --env dev
```
**Expected**: Prints structured report showing files scanned, union/intersection columns, any extra/missing columns.

---

## Step 4: Implement Variant Detection

Detect similar column names that might be the same column with different formatting:

```python
def normalize_name(name: str) -> str:
    """Normalize column name for comparison."""
    return ''.join(c.lower() for c in name if c.isalnum())

def find_variants(columns: set[str]) -> list[list[str]]:
    """Find columns that look similar (potential duplicates/typos)."""
    norm_map = {}  # normalized -> [original names]
    for col in columns:
        norm = normalize_name(col)
        if norm not in norm_map:
            norm_map[norm] = []
        norm_map[norm].append(col)

    # Return groups with more than one variant
    return [sorted(names) for names in norm_map.values() if len(names) > 1]
```

Add to report output:
```
POSSIBLE DUPLICATES (similar names):
  "EffectiveDate" vs "Effective Date"
  "AcctExec" vs "Acct Exec"
```

### Validation Checkpoint 4
```bash
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset tasks --env dev
```
**Expected**: Report includes "POSSIBLE DUPLICATES" section (may be empty if no variants exist).

---

## Step 5: Implement --baseline Flag

Compare all files against one specific baseline file:

```bash
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset tasks --env dev --baseline "Jan 2025.xlsx"
```

Output format:
```
BASELINE: Jan 2025.xlsx (21 columns)

MATCHES BASELINE:
  Feb2025.xlsx, March2025.xlsx (2 files)

DIFFERS FROM BASELINE:
  Jan2026 1.xlsx:
    + NewColumn1 (extra)
    + NewColumn2 (extra)
  Feb 2026.xlsx:
    + NewColumn1 (extra)
```

### Validation Checkpoint 5
```bash
# Use first file as baseline
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset tasks --env dev --baseline "sample_tasks.xlsx"
```
**Expected**: Shows baseline comparison. Files matching baseline listed together, differences shown per-file.

---

## Step 6: Implement --check-against Flag

Compare raw Excel files against schema.yaml expectations:

```bash
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset tasks --env dev --check-against
```

This reads `config/schema.yaml` from the dataset and compares:
- `column_order` — expected columns after normalization
- `column_aliases` — maps Excel headers to canonical names

Logic:
1. Load schema.yaml, get `column_order` and `column_aliases`
2. **Filter out pipeline-generated columns** — these are added during processing, not expected in source Excel:
   - `_source_file`, `_ingested_at`, `source_system`, `row_id`
3. For each Excel file, check if headers match expected (considering aliases)
4. Report:
   - Columns in schema but missing from file (will be added as NULL)
   - Columns in file but not in schema (will be dropped)
   - Which alias mappings are being used

```python
def check_against_schema(raw_dir: Path, schema_path: Path) -> dict:
    """Compare raw files against schema.yaml expectations."""
    import yaml
    with open(schema_path) as f:
        schema = yaml.safe_load(f)

    column_order = schema.get("column_order", [])
    aliases = schema.get("column_aliases", {})

    # Filter out pipeline-generated columns (not expected in source Excel files)
    pipeline_generated = {"_source_file", "_ingested_at", "source_system", "row_id"}
    expected = set(col for col in column_order if col not in pipeline_generated)

    # ... comparison logic
```

### Validation Checkpoint 6
```bash
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset tasks --env dev --check-against
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset launch --env dev --check-against
```
**Expected**:
- tasks: Shows comparison against schema, lists any unmapped columns
- launch: Shows alias mappings being used (e.g., "First & Last Name" → name)

---

## Step 7: Implement --output Flag

Write full report as structured JSON:

```bash
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset tasks --env dev --output schema_report.json
```

```python
if args.output:
    import json
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Report written to: {args.output}")
```

### Validation Checkpoint 7
```bash
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset tasks --env dev --output /tmp/test_report.json
.venv/Scripts/python.exe -c "import json; print(json.load(open('/tmp/test_report.json'))['file_count'])"
```
**Expected**: JSON file created, can be parsed, contains expected fields.

---

## Step 8: Integrate --preflight into run_pipeline.py

Add `--preflight` flag to `run_pipeline.py`:

```python
parser.add_argument(
    "--preflight",
    action="store_true",
    help="Run schema comparison before pipeline. Warns on extra columns, errors if schema columns missing from ALL files.",
)
```

Before step 01, run schema check:

```python
def run_preflight(dataset_root: Path, log: logging.Logger) -> bool:
    """Run schema preflight check. Returns True if OK to proceed."""
    from scripts.compare_schemas import compare_schemas, check_against_schema

    raw_dir = dataset_root / "raw"
    schema_path = dataset_root / "config" / "schema.yaml"

    if not raw_dir.exists() or not any(raw_dir.glob("*.xlsx")):
        log.info("Preflight: No Excel files in raw/, skipping")
        return True

    result = check_against_schema(raw_dir, schema_path)

    # Warn on extra columns (step 02 will drop them)
    if result.get("extra_in_files"):
        log.warning("Preflight: Extra columns in source files (will be dropped): %s",
                    result["extra_in_files"])

    # Error if schema expects columns missing from ALL files
    if result.get("missing_from_all_files"):
        log.error("Preflight FAILED: Schema expects columns missing from ALL source files: %s",
                  result["missing_from_all_files"])
        log.error("Check schema.yaml or column_aliases configuration")
        return False

    log.info("Preflight: Schema check passed")
    return True
```

In `run_single_dataset()`, before running steps:
```python
if args.preflight:
    if not run_preflight(dataset_root, log):
        return 1
```

### Validation Checkpoint 8
```bash
# Should pass
.venv/Scripts/python.exe run_pipeline.py --dataset tasks --preflight --dry-run

# Test all datasets
.venv/Scripts/python.exe run_pipeline.py --dataset tasks --preflight --dry-run
.venv/Scripts/python.exe run_pipeline.py --dataset employees_master --preflight --dry-run
.venv/Scripts/python.exe run_pipeline.py --dataset launch --preflight --dry-run
```
**Expected**:
- Preflight runs before dry-run validation
- Prints "Schema check passed" or warnings about extra columns
- Does NOT modify any files

---

## Step 9: Test All Datasets

Run compare_schemas against every dataset:

```bash
# All 6 datasets
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset tasks --env dev
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset dept_mapping --env dev
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset employees_master --env dev
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset workers --env dev
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset launch --env dev
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset revenue --env dev

# With schema check
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset tasks --env dev --check-against
.venv/Scripts/python.exe scripts/compare_schemas.py --dataset launch --env dev --check-against
```

### Validation Checkpoint 9
**Expected**:
- All commands complete without Python errors
- Each dataset shows file count, column analysis
- `--check-against` shows schema comparison
- Execution time < 2 seconds per dataset

---

## Step 10: Update ALL_DATASETS in run_pipeline.py

Update the datasets list to include all 6:

```python
ALL_DATASETS = ["tasks", "dept_mapping", "employees_master", "workers", "launch", "revenue"]
```

### Validation Checkpoint 10
```bash
.venv/Scripts/python.exe run_pipeline.py --all --preflight --dry-run
```
**Expected**: Runs preflight + dry-run for all 6 datasets.

---

## Build Rules

1. **One file**: `scripts/compare_schemas.py` — no new modules in `lib/`
2. **Header-only reads** — must complete in under 2 seconds for 20 files
3. **No new dependencies** — openpyxl and pyyaml are already installed
4. **No behavior changes** — existing pipeline commands work unchanged without `--preflight`
5. **Use existing patterns** — follow `lib/data_root.py` for path resolution

---

## Files to Create/Modify

| File | Action |
|------|--------|
| `scripts/compare_schemas.py` | CREATE — main tool (~300 lines) |
| `run_pipeline.py` | MODIFY — add `--preflight` flag, update `ALL_DATASETS` |

---

## Success Criteria

- [ ] Standalone script runs against all 6 datasets without errors
- [ ] Correctly identifies extra/missing columns
- [ ] Correctly identifies column name variants
- [ ] `--check-against` validates against schema.yaml including aliases
- [ ] `--baseline` compares against specific file
- [ ] `--output` produces valid JSON
- [ ] `--preflight` stops pipeline when schema is fundamentally broken
- [ ] `--preflight` warns but continues when extra columns found
- [ ] Under 2 seconds execution time per dataset
- [ ] All existing tests still pass
- [ ] No changes to existing pipeline behavior without `--preflight` flag
