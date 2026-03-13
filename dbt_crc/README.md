# dbt_crc (crc_analytics)

dbt project for CRC analytics. Connects to DuckDB (dev: `../powerbi/dev_warehouse.duckdb`, prod: `../powerbi/warehouse.duckdb`).

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

```bash
cd dbt_crc
dbt seed
dbt run
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
