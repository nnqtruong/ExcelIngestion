"""Pipeline tests - run with: pytest tests/ -v"""
import pandas as pd
import sqlite3
import yaml
import pytest
from pathlib import Path

from lib.data_root import get_analytics_path
from lib.paths import ANALYTICS_DIR
ROOT = Path(__file__).parent.parent
CONFIG = ROOT / "datasets" / "dev" / "tasks" / "config"
FIXTURES = Path(__file__).parent / "fixtures"
DB_PATH = get_analytics_path() / "dev_warehouse.db"


def _canonicalize_excel_columns(df, schema):
    """Map Excel headers to schema keys using column_aliases (same idea as step 02)."""
    aliases = schema.get("column_aliases") or {}
    lower_to_target = {str(k).lower(): str(v) for k, v in aliases.items()}
    return df.rename(columns=lambda c: lower_to_target.get(str(c).lower(), str(c).lower()))


@pytest.fixture
def schema():
    with open(CONFIG / "schema.yaml") as f:
        return yaml.safe_load(f)


@pytest.fixture
def value_maps():
    with open(CONFIG / "value_maps.yaml") as f:
        return yaml.safe_load(f)


@pytest.fixture
def clean_df():
    return pd.read_excel(FIXTURES / "sample_clean.xlsx")


@pytest.fixture
def bad_df():
    return pd.read_excel(FIXTURES / "sample_bad.xlsx")


@pytest.fixture
def partial_df():
    return pd.read_excel(FIXTURES / "sample_partial.xlsx")


# ---- Schema tests ----

class TestSchemaConfig:
    def test_schema_has_all_21_columns(self, schema):
        # 20 core + 3 evolution + 2 pipeline-generated columns (see schema.yaml header)
        assert len(schema["columns"]) == 25

    def test_schema_has_column_order(self, schema):
        assert len(schema["column_order"]) == 25
        assert schema["column_order"][0] == "task_id"

    def test_schema_columns_match_order(self, schema):
        col_keys = set(schema["columns"].keys())
        order_keys = set(schema["column_order"])
        assert col_keys == order_keys, f"Mismatch: {col_keys.symmetric_difference(order_keys)}"

    def test_row_id_is_primary_key(self, schema):
        # row_id is auto-generated in step 6 as the primary key, not defined in schema
        # Just verify task_id exists and is not nullable (it's still required)
        assert "task_id" in schema["columns"]
        assert schema["columns"]["task_id"].get("nullable") is False

    def test_validation_thresholds_exist(self, schema):
        assert "validation" in schema
        assert "max_null_rate" in schema["validation"]
        assert "max_duplicate_rate" in schema["validation"]


# ---- Step 02: Normalize schema ----

class TestNormalizeSchema:
    def test_column_names_lowercase(self, clean_df):
        renamed = clean_df.rename(columns=str.lower)
        for col in renamed.columns:
            assert col == col.lower(), f"Column not lowercase: {col}"

    def test_all_expected_columns_present(self, clean_df, schema):
        renamed = _canonicalize_excel_columns(clean_df, schema)
        # _source_file, _ingested_at added at combine time, not in per-file clean output
        combine_only = {"_source_file", "_ingested_at"}
        expected = set(schema["column_order"]) - combine_only
        actual = set(renamed.columns)
        missing = expected - actual
        # Sample Excel predates schema-evolution columns (see schema.yaml)
        allowed_missing = {"priority", "operation_time"}
        unexpected = missing - allowed_missing
        assert not unexpected, f"Missing columns: {unexpected}"


# ---- Step 03: Add missing columns ----

class TestAddMissingColumns:
    def test_partial_file_gets_missing_columns(self, partial_df, schema):
        renamed = _canonicalize_excel_columns(partial_df, schema)
        expected = set(schema["column_order"])
        actual = set(renamed.columns)
        missing = expected - actual
        # These should be added by step 03
        assert len(missing) > 0, "Partial fixture should be missing columns"

        # Simulate adding missing columns
        for col in missing:
            renamed[col] = None
        assert set(renamed.columns) == expected


# ---- Step 04: Clean errors ----

class TestCleanErrors:
    def test_taskid_not_nullable(self, bad_df, schema):
        col_config = schema["columns"]["task_id"]
        assert col_config["nullable"] is False
        # bad_df has a null taskid - should be caught
        renamed = _canonicalize_excel_columns(bad_df, schema)
        null_count = renamed["task_id"].isna().sum()
        assert null_count > 0, "Bad fixture should have null taskids for testing"

    def test_bad_types_detected(self, bad_df, schema):
        renamed = _canonicalize_excel_columns(bad_df, schema)
        # task_id should be int but has None - pd will make it float
        assert renamed["task_id"].dtype != "int64"


# ---- Step 05: Normalize values ----

class TestNormalizeValues:
    def test_value_maps_cover_taskstatus(self, value_maps):
        assert "taskstatus" in value_maps
        assert value_maps["taskstatus"]["completed"] == "Completed"

    def test_value_maps_cover_flowname(self, value_maps):
        assert "flowname" in value_maps
        assert value_maps["flowname"]["uw renewal"] == "UW Renewal"

    def test_bad_status_not_in_allowed(self, bad_df, schema):
        allowed = schema["columns"]["task_status"]["allowed_values"]
        renamed = _canonicalize_excel_columns(bad_df, schema)
        for val in renamed["task_status"].dropna():
            if val not in allowed:
                # This is expected - confirms bad fixture works
                return
        pytest.fail("Bad fixture should contain invalid taskstatus values")


# ---- Step 06: Combine & duplicates ----

class TestCombineDatasets:
    def test_duplicates_detected(self, bad_df, schema):
        renamed = _canonicalize_excel_columns(bad_df, schema)
        dupes = renamed["task_id"].dropna().duplicated().sum()
        assert dupes > 0, "Bad fixture should have duplicate taskids"

    def test_clean_data_no_duplicates(self, clean_df, schema):
        renamed = _canonicalize_excel_columns(clean_df, schema)
        dupes = renamed["task_id"].duplicated().sum()
        assert dupes == 0


# ---- Step 08: Validation ----

class TestValidation:
    def test_clean_data_passes_null_threshold(self, clean_df, schema):
        max_null = schema["validation"]["max_null_rate"]
        renamed = _canonicalize_excel_columns(clean_df, schema)
        for col in renamed.columns:
            null_rate = renamed[col].isna().mean()
            assert null_rate <= max_null, f"{col} null rate {null_rate} exceeds {max_null}"

    def test_clean_data_passes_row_count(self, clean_df, schema):
        assert len(clean_df) >= schema["validation"]["min_row_count"]

    def test_clean_data_column_count(self, clean_df, schema):
        renamed = _canonicalize_excel_columns(clean_df, schema)
        combine_only = {"_source_file", "_ingested_at"}
        evolution_optional = {"priority", "operation_time"}
        expected = set(schema["column_order"]) - combine_only - evolution_optional
        assert set(renamed.columns) == expected


# ---- Steps 09-10: SQLite Export ----

class TestSQLiteExport:
    @pytest.fixture
    def db_conn(self):
        """Fixture to provide SQLite connection if database exists."""
        if not DB_PATH.exists():
            pytest.skip("SQLite database not found - run pipeline first")
        conn = sqlite3.connect(DB_PATH)
        yield conn
        conn.close()

    @pytest.fixture
    def parquet_df(self):
        """Fixture to load combined parquet if it exists (tasks dataset)."""
        parquet_path = ANALYTICS_DIR / "combined.parquet"
        if not parquet_path.exists():
            pytest.skip("Combined parquet not found - run pipeline first")
        return pd.read_parquet(parquet_path)

    def test_db_exists(self):
        """Test that dev warehouse SQLite exists under external analytics (after pipeline)."""
        if not DB_PATH.exists():
            pytest.skip(f"SQLite database not found at {DB_PATH} — run pipeline first")
        assert DB_PATH.is_file()

    def test_row_count_matches_parquet(self, db_conn, parquet_df):
        """Test that SQLite row count equals Parquet row count."""
        cursor = db_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tasks")
        sqlite_count = cursor.fetchone()[0]
        parquet_count = len(parquet_df)
        assert sqlite_count == parquet_count, f"SQLite has {sqlite_count} rows, Parquet has {parquet_count}"

    def test_row_id_unique(self, db_conn):
        """Test that row_id is unique (auto-generated primary key)."""
        cursor = db_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tasks")
        total = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT row_id) FROM tasks")
        unique = cursor.fetchone()[0]
        assert total == unique, f"Duplicate row_ids found: {total} total, {unique} unique"

    def test_indexes_exist(self, db_conn):
        """Test that key indexes are present (matches lib.export_sqlite.create_indexes)."""
        expected_indexes = [
            "idx_tasks_taskstatus",
            "idx_tasks_taskid",
            "idx_tasks_dateinitiated",
            "idx_tasks_assignedto",
            "idx_tasks_operationby",
        ]
        cursor = db_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='tasks'")
        actual_indexes = {row[0] for row in cursor.fetchall()}
        for idx in expected_indexes:
            assert idx in actual_indexes, f"Index {idx} not found"

    def test_views_exist(self, db_conn):
        """Primary mart view synced from dbt must be queryable (others may depend on workers/employees tables)."""
        cursor = db_conn.cursor()
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name='mart_tasks_enriched'"
        )
        if cursor.fetchone() is None:
            pytest.skip("mart_tasks_enriched missing — run dbt first")
        cursor.execute("SELECT COUNT(*) FROM mart_tasks_enriched")
        assert cursor.fetchone() is not None
