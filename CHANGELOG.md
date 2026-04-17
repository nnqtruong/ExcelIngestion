# Changelog

All notable changes to the Excel Ingestion Pipeline are documented here.

## [2026-04-17] - Data Recovery Sprint

### Added

- **`null_strings` schema feature (step 04)**
  - Per-column `null_strings` list in `dataset.yaml` to treat literal strings as NULL before type casting
  - `lib/clean_errors.py`: New `_pre_cast_varchar_expr()` builds nested `NULLIF` chain
  - `_cast_expr()`, `_good_cond()`, `_bad_cond()` now accept `null_strings` parameter
  - Tasks datetime columns configured: `["NULL", "null", "N/A", ""]` (date_available also includes `"Completed"`)
  - Prevents rows with literal "NULL" strings from being flagged as cast errors

- **Chunked SQLite export (step 09)**
  - `lib/export_sqlite.py`: New `export_to_sqlite_chunked()` using PyArrow `iter_batches()`
  - Threshold: Parquet files > 50MB use chunked export (100K rows per batch)
  - Memory stays bounded regardless of file size (fixes 2GB RAM spike on 6M rows)
  - Progress logging every 500K rows
  - Indexes created after all chunks written

### Changed

- **Tasks `file_number` dtype: `int64` → `string`**
  - AMS exports contain alphanumeric identifiers (e.g., "~ER7258868", "568SU-5896")
  - Storing as string avoids step 04 cast failures and row loss to `errors/`
  - Updated in both `ExcelIngestion_Data/dev/tasks/dataset.yaml` and `prod/tasks/dataset.yaml`

### Fixed

- Literal "NULL" strings in datetime columns no longer cause entire rows to be flagged as errors
- Alphanumeric file_number values no longer cause cast failures

---

## [2026-04-17] - Pipeline Hardening Sprint

### Added

- **Unified Configuration (`dataset.yaml`)**
  - Single YAML file per dataset merging `pipeline.yaml`, `schema.yaml`, `value_maps.yaml`, and `combine.yaml`
  - New `load_dataset_config()` function in `lib/config.py` with legacy fallback
  - Created `dataset.yaml` for all 6 datasets in both dev and prod environments
  - Reduces config file sprawl from 4 files to 1 per dataset

- **SQL Utilities Module (`lib/sql_utils.py`)**
  - Centralized `escape_sql_string()` and `quote_identifier()` functions
  - Eliminated duplicate SQL escape implementations across 7 lib modules:
    - `lib/combine_datasets.py`
    - `lib/normalize_schema.py`
    - `lib/add_missing_columns.py`
    - `lib/clean_errors.py`
    - `lib/normalize_values.py`
    - `lib/handle_nulls.py`
    - `lib/validate.py`

- **One-Command Refresh (`refresh.py`)**
  - Single script to run pipeline + dbt in one command
  - Automatically uses `.venv-dbt/Scripts/dbt.exe` for dbt (no venv switching needed)
  - Flags: `--env`, `--dataset`, `--all`, `--skip-pipeline`, `--skip-dbt`, `--force`
  - Proper error handling and exit codes

- **Documentation**
  - `docs/config_audit.md` - Configuration consolidation audit report
  - Updated `docs/current_state.md` with unified config section
  - Updated `docs/USER_GUIDE.md` with One-Command Refresh section
  - Updated `README.md` Quick Start with `refresh.py` usage

### Changed

- `lib/config.py` now supports both unified `dataset.yaml` and legacy split files
- All lib modules now import SQL utilities from `lib/sql_utils.py`
- Snake_case column naming enforced throughout pipeline

### Known Issues

- **dbt models need update**: dbt staging models reference old column names (e.g., `taskstatus` instead of `task_status`). Pipeline outputs correct snake_case columns; dbt models need corresponding update.
- **SQLite index test**: One pre-existing test failure (`test_indexes_exist`) - indexes not configured in pipeline.yaml.

---

## [2026-04-16] - Schema Comparison Tool

### Added

- `scripts/compare_schemas.py` - Compare Excel headers across files
- `--preflight` flag for `run_pipeline.py` - Validate schemas before processing
- `--check-against` option to compare against schema.yaml
- `--baseline` option to compare files against a specific baseline file

---

## [2026-04-10] - Incremental Processing

### Added

- File fingerprinting with MD5 hashes in `lib/fingerprint.py`
- Incremental processing - skip unchanged files in step 01
- State tracking in `_state/ingestion_state.json` per dataset
- `--force` flag to reprocess all files regardless of fingerprint

---

## [2026-04-01] - External Data Directory

### Added

- `scripts/init_data_directory.py` - Create external data folder structure
- `scripts/migrate_data.py` - Migrate legacy in-repo data
- `DATA_ROOT` environment variable support
- External data persists across code updates (zip replacement)

### Changed

- Default data location: `../ExcelIngestion_Data` (sibling to repo)
- SQLite and DuckDB databases now under `{DATA_ROOT}/analytics/` and `{DATA_ROOT}/powerbi/`

---

## [2025-12-15] - dbt Analytics Layer

### Added

- dbt project in `dbt_crc/` with staging views and mart models
- Separate `.venv-dbt` for Python 3.12 compatibility
- Marts: `mart_tasks_enriched`, `mart_team_capacity`, `mart_team_demand`, `mart_onshore_offshore`, `mart_backlog`, `mart_turnaround`, `mart_daily_trend`

### Changed

- Pipeline stops at step 09 (SQLite export)
- All analytics now handled by dbt in DuckDB

---

## [2025-11-01] - Initial Release

### Added

- 9-step pipeline: Excel to Parquet to SQLite
- Multi-dataset support (tasks, dept_mapping, employees_master, workers, revenue, launch)
- Column aliasing for multi-schema sources
- Dev/prod environment separation
- Power BI integration via DuckDB ODBC
- Chunked Excel processing (50K rows per chunk)
- Error row extraction to `errors/` directory
- Validation with JSON reports
