# Config loading architecture — assessment audit

**Purpose**: Task 1 discovery for config consolidation and related refactors. No production code was changed for this document.

**Data source**: Default external root `../ExcelIngestion_Data` (resolved on this machine to `C:\Users\quang\CRC Code\ExcelIngestion_Data`).

**Validation (2026-04-17)**:

- `python run_pipeline.py --dataset tasks --dry-run` — **passed** (config YAML parse + layout checks).
- `python -m pytest tests/ -q` — **8 failed, 15 passed** (failures appear tied to fixture/schema expectations vs current `datasets/` or env; not introduced by this audit-only change).

---

## Section 1: Config loader functions

Scope: `lib/config.py`, `lib/schema.py`, `lib/paths.py` as requested.

### `lib/config.py`

| Function | Signature | File(s) loaded | Returns | Callers |
|----------|-----------|----------------|---------|---------|
| `load_pipeline_config` | `(dataset_root: Path) -> dict` | `{dataset_root}/pipeline.yaml` (UTF-8); missing file → `{}` | Parsed YAML dict or `{}` | **Internal only**: `get_sqlite_config` |
| `get_sqlite_config` | `(dataset_root: Path) -> dict` | Same as above (via `load_pipeline_config`) | `{"database": str, "table": str}` with defaults `tasks.db` / `tasks` | **Internal**: `get_sqlite_table_name`, `get_sqlite_db_path` |
| `get_sqlite_table_name` | `(dataset_root: Path) -> str` | `pipeline.yaml` (via `get_sqlite_config`) | SQLite table name | `scripts/09_export_sqlite.py` |
| `get_sqlite_db_path` | `(dataset_root: Path) -> Path` | `pipeline.yaml` (via `get_sqlite_config`) + `PIPELINE_ENV`, `lib.data_root.get_analytics_path` | Path to warehouse `.db` | `scripts/09_export_sqlite.py`, `lib/paths.get_sqlite_path` |
| `load_combine_config` | `(path: Path) -> dict` | Path given (expected `config/combine.yaml`); missing → `{}` | Parsed YAML or `{}` | **Internal**: `get_combined_path` |
| `get_combined_path` | `(analytics_dir: Path, combine_config_path: Path) -> Path` | `combine.yaml` at `combine_config_path` (via `load_combine_config`) | `analytics_dir / output_filename` (`combined.parquet` default) | `run_pipeline.py` (`get_analytics_output_path`), `scripts/06_combine_datasets.py`, `scripts/07_handle_nulls.py`, `scripts/08_validate.py`, `scripts/09_export_sqlite.py`, `lib/combine_datasets.run_combine_datasets` (via `get_combined_path` only) |

**Note**: `load_pipeline_config` is not imported elsewhere; orchestration discovers the dataset via `pipeline.yaml` path in `run_pipeline.py`, but SQLite settings are the only runtime consumers of parsed `pipeline.yaml` today.

### `lib/schema.py`

| Function | Signature | File(s) loaded | Returns | Callers |
|----------|-----------|----------------|---------|---------|
| `load_schema` | `(path: Path) -> dict` | Any path passed (convention: `config/schema.yaml`); raises `FileNotFoundError` / `ValueError` if invalid | Full schema dict (must include `columns`) | `lib/convert.py`, `lib/normalize_schema.py`, `lib/add_missing_columns.py`, `lib/clean_errors.py`, `lib/handle_nulls.py`, `lib/validate.py`, `scripts/01_convert.py` |
| `columns_as_list` | `(schema: dict) -> list[dict]` | *(none — in-memory)* | Normalized column specs | Used by multiple `lib/*` modules after `load_schema` |
| `get_column_order` | `(schema: dict) -> list[str]` | *(none)* | Ordered column names | `lib/add_missing_columns.py`, etc. |
| `get_column_aliases` | `(schema: dict) -> dict[str, str]` | *(none)* | Alias map | `lib/normalize_schema.py`, etc. |

Typical path argument: `lib.paths.SCHEMA_PATH` → `{DATASET_ROOT}/config/schema.yaml`.

### `lib/paths.py`

**No functions load YAML.** This module resolves roots and exposes path constants used with loaders elsewhere:

| Name | Role |
|------|------|
| `SCHEMA_PATH` | `CONFIG_DIR / "schema.yaml"` |
| `COMBINE_PATH` | `CONFIG_DIR / "combine.yaml"` |
| `VALUE_MAPS_PATH` | `CONFIG_DIR / "value_maps.yaml"` |
| `get_sqlite_path()` | Delegates to `lib.config.get_sqlite_db_path(get_dataset_root())` (reads `pipeline.yaml` indirectly) |

### Related YAML loading (outside the three files, for consolidation planning)

- `lib/normalize_values.load_value_maps(path)` — loads `value_maps.yaml`.
- `lib/validate.run_validate` — opens `combine_config_path` and uses `yaml.safe_load` directly (parallel to `load_combine_config` behavior for optional file).
- `run_pipeline.dry_run_validate` — parses `schema.yaml`, `combine.yaml`, optional `value_maps.yaml` with `yaml.safe_load` for syntax checks only.

---

## Section 2: Config file references (`lib/` + `scripts/`)

Commands used: ripgrep equivalents of the requested patterns, scoped to `lib/` and `scripts/*.py`.

### `pipeline.yaml`

| File | Role |
|------|------|
| `lib/config.py` | Loads via `load_pipeline_config`; docstrings reference filename |
| `scripts/init_data_directory.py` | Copies template `pipeline.yaml` into `DATA_ROOT`; CLI help text |

### `schema.yaml`

| File | Role |
|------|------|
| `lib/schema.py` | Docstring / `load_schema` describes schema file |
| `lib/paths.py` | `SCHEMA_PATH` constant |
| `lib/convert.py` | Docstring mentions step 04 / schema |
| `scripts/init_data_directory.py` | Help text |
| `scripts/compare_schemas.py` | Docstring, default path `dataset_root / "config" / "schema.yaml"`, CLI help |

### `value_maps.yaml`

| File | Role |
|------|------|
| `lib/paths.py` | `VALUE_MAPS_PATH` |
| `lib/normalize_values.py` | Docstring; `load_value_maps` error messages |
| `scripts/05_normalize_values.py` | Module docstring |

### `combine.yaml`

| File | Role |
|------|------|
| `lib/paths.py` | `COMBINE_PATH` |
| `lib/config.py` | Docstrings; `load_combine_config` / `get_combined_path` |
| `lib/validate.py` | User-facing skip message references `combine.yaml` |
| `scripts/init_data_directory.py` | Help text |

**Out of scope for the grep list but relevant**: `run_pipeline.py` (repo root) references all four concepts for dry-run, preflight, and path resolution; `tests/test_pipeline.py` loads fixture YAMLs.

---

## Section 3: SQL escape / identifier helpers

### `def _escape_sql`

| File | Implementation | Docstring |
|------|------------------|-----------|
| `lib/normalize_schema.py` | `return s.replace("'", "''")` | Yes — “Escape single quotes…” |
| `lib/validate.py` | Same | No |
| `lib/handle_nulls.py` | Same | No |
| `lib/add_missing_columns.py` | Same | No |
| `lib/clean_errors.py` | Same | No |
| `lib/normalize_values.py` | `return str(s).replace("'", "''")` | No |
| `lib/combine_datasets.py` | `return s.replace("'", "''")` | Yes |

**Comparison**: All paths escape single quotes for SQL string literals the same way; `normalize_values` additionally coerces with `str(s)` (relevant if non-string keys/values ever appear).

**Usage**: Each definition is used only within its own module (no shared import).

### `def _quote_id`

| File | Implementation |
|------|----------------|
| `lib/normalize_schema.py` | `'"' + name.replace('"', '""') + '"'` |
| `lib/validate.py` | Same |
| `lib/handle_nulls.py` | Same |
| `lib/add_missing_columns.py` | Same |
| `lib/clean_errors.py` | Same |
| `lib/normalize_values.py` | Same |

**Not defined in**: `lib/combine_datasets.py` (uses double-quoted column name inline only for optional `primary_key` duplicate check).

**Comparison**: Implementations are identical across the six files.

**Usage**: Private to each module; no `escape_sql` / `quote_id` public names in `lib/` or `scripts/`.

### Misc

- `lib/combine_datasets.py` imports `load_combine_config` but **does not call it** (dead import as of this audit).

---

## Section 4: Dataset config inventory

Inventory command (PowerShell equivalent of the sprint’s bash loop): listed `../ExcelIngestion_Data/dev/<dataset>/` and `config/` for each dataset. All six datasets include `pipeline.yaml` at dataset root and `config/schema.yaml`, `config/combine.yaml`, `config/value_maps.yaml`.

Counts below: **pipeline** = non-blank lines in `pipeline.yaml`; **schema cols** = number of entries under `columns` (list or dict form); **value map cols** = top-level keys in parsed YAML (`0` if file is comments-only / parses to `null`).

| Dataset | pipeline.yaml | schema.yaml | value_maps.yaml | combine.yaml |
|---------|-----------------|-------------|-----------------|---------------|
| tasks | ✓ (15 lines) | ✓ (25 cols) | ✓ (3 maps) | ✓ |
| dept_mapping | ✓ (23 lines) | ✓ (13 cols) | ✓ (2 maps) | ✓ |
| employees_master | ✓ (33 lines) | ✓ (23 cols) | ✓ (3 maps) | ✓ |
| workers | ✓ (21 lines) | ✓ (34 cols) | ✓ (3 maps) | ✓ |
| revenue | ✓ (23 lines) | ✓ (11 cols) | ✓ (file present, **0** maps — comments only) | ✓ |
| launch | ✓ (23 lines) | ✓ (11 cols) | ✓ (file present, **0** maps — comments only) | ✓ |

Repo templates under `datasets/dev/<dataset>/` mirror the same layout for `init_data_directory.py`.

---

## Section 5: `refresh.py` and dbt executable

| Question | Finding |
|----------|---------|
| `refresh.py` at project root? | **No** — not present. |
| `powerbi/refresh.py`? | **No** — not present under `powerbi/`. |
| Exact dbt path (Windows, this repo) | `.venv-dbt\Scripts\dbt.exe` (absolute: `C:\Users\quang\CRC Code\ExcelIngestion\.venv-dbt\Scripts\dbt.exe`) |

### Subprocess smoke test

Executed from repo root with `.venv-dbt`’s Python:

```python
import subprocess
from pathlib import Path
p = Path(".venv-dbt/Scripts/dbt.exe")
result = subprocess.run([str(p), "--version"], capture_output=True)
```

**Result**: `returncode == 0`; stdout reported **Core installed 1.11.7** and **duckdb plugin 1.10.1** (minor “update available” notice to 1.11.8).

---

## Checklist (Task 1)

- [x] `docs/config_audit.md` exists  
- [x] All five sections complete  
- [x] No production code modified (documentation only)  
- [ ] Findings reviewed with senior engineer before proceeding (human gate)
