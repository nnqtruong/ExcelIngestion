# dbt_crc (crc_analytics)

dbt project for CRC analytics. DuckDB files are under **`{DATA_ROOT}/powerbi/`** (same external root as the Python pipeline). Set the **`DATA_ROOT`** environment variable to your data folder (e.g. `C:\...\ExcelIngestion_Data`), or rely on the default in `profiles.yml` (`../../ExcelIngestion_Data` relative to this directory when you run `cd dbt_crc` first). See comments at the top of `profiles.yml`.

## Python version (required)

**dbt does not support Python 3.13+ yet.** Use **Python 3.10, 3.11, or 3.12** only.

If your default is 3.14 or 3.13, create a separate venv with 3.12:

```bash
# Install Python 3.12 from python.org if needed, then:
py -3.12 -m venv .venv-dbt
.venv-dbt\Scripts\activate
pip install dbt-core dbt-duckdb
```

Then from this directory:

```bash
dbt debug
```

Do not proceed with dbt until `dbt debug` shows **All checks passed!**

## dbt Cloud

This project runs **locally only**. There is no dbt Cloud setup. Ignore any `dbt_cloud.yml` or dbt Cloud credential warnings; they are irrelevant when running the CLI locally.

## Quick start (after venv and dbt debug)

Use the same **`DATA_ROOT`** as the Python pipeline (default: `../../ExcelIngestion_Data` when you `cd dbt_crc`). PowerShell: `$env:DATA_ROOT="C:\path\to\ExcelIngestion_Data"`.

```bash
cd dbt_crc
dbt seed    # required once per new DuckDB file (value maps)
dbt run     # needs combined Parquet under DATA_ROOT for each source; use --select if you only have some datasets
dbt test
```

## Validate stg_tasks (Prompt 5)

From project root with dbt venv activated (e.g. `.venv-dbt\Scripts\activate`):

```bash
cd dbt_crc
dbt run --select stg_tasks
dbt test --select stg_tasks
```

Optional (requires `dbt-codegen` package):  
`dbt run-operation generate_model_yaml --args '{"model_names": ["stg_tasks"]}'`
