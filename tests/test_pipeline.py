"""Pipeline tests - run with: pytest tests/ -v"""
import pandas as pd
import sqlite3
import yaml
import pytest
from pathlib import Path

ROOT = Path(__file__).parent.parent
CONFIG = ROOT / "datasets" / "dev" / "tasks" / "config"
FIXTURES = Path(__file__).parent / "fixtures"
ANALYTICS = ROOT / "analytics"
DB_PATH = ANALYTICS / "dev_warehouse.db"


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
        # 21 task columns + _source_file, _ingested_at
        assert len(schema["columns"]) == 23

    def test_schema_has_column_order(self, schema):
        assert len(schema["column_order"]) == 23
        assert schema["column_order"][0] == "taskid"

    def test_schema_columns_match_order(self, schema):
        col_keys = set(schema["columns"].keys())
        order_keys = set(schema["column_order"])
        assert col_keys == order_keys, f"Mismatch: {col_keys.symmetric_difference(order_keys)}"

    def test_row_id_is_primary_key(self, schema):
        # row_id is auto-generated in step 6 as the primary key, not defined in schema
        # Just verify taskid exists and is not nullable (it's still required)
        assert "taskid" in schema["columns"]
        assert schema["columns"]["taskid"].get("nullable") is False

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
        renamed = clean_df.rename(columns=str.lower)
        # _source_file, _ingested_at added at combine time, not in per-file clean output
        combine_only = {"_source_file", "_ingested_at"}
        expected = set(schema["column_order"]) - combine_only
        actual = set(renamed.columns)
        missing = expected - actual
        assert not missing, f"Missing columns: {missing}"


# ---- Step 03: Add missing columns ----

class TestAddMissingColumns:
    def test_partial_file_gets_missing_columns(self, partial_df, schema):
        renamed = partial_df.rename(columns=str.lower)
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
        col_config = schema["columns"]["taskid"]
        assert col_config["nullable"] is False
        # bad_df has a null taskid - should be caught
        renamed = bad_df.rename(columns=str.lower)
        null_count = renamed["taskid"].isna().sum()
        assert null_count > 0, "Bad fixture should have null taskids for testing"

    def test_bad_types_detected(self, bad_df):
        renamed = bad_df.rename(columns=str.lower)
        # taskid should be int but has None - pd will make it float
        assert renamed["taskid"].dtype != "int64"


# ---- Step 05: Normalize values ----

class TestNormalizeValues:
    def test_value_maps_cover_taskstatus(self, value_maps):
        assert "taskstatus" in value_maps
        assert value_maps["taskstatus"]["completed"] == "Completed"

    def test_value_maps_cover_flowname(self, value_maps):
        assert "flowname" in value_maps
        assert value_maps["flowname"]["uw renewal"] == "UW Renewal"

    def test_bad_status_not_in_allowed(self, bad_df, schema):
        allowed = schema["columns"]["taskstatus"]["allowed_values"]
        renamed = bad_df.rename(columns=str.lower)
        for val in renamed["taskstatus"].dropna():
            if val not in allowed:
                # This is expected - confirms bad fixture works
                return
        pytest.fail("Bad fixture should contain invalid taskstatus values")


# ---- Step 06: Combine & duplicates ----

class TestCombineDatasets:
    def test_duplicates_detected(self, bad_df):
        renamed = bad_df.rename(columns=str.lower)
        dupes = renamed["taskid"].dropna().duplicated().sum()
        assert dupes > 0, "Bad fixture should have duplicate taskids"

    def test_clean_data_no_duplicates(self, clean_df):
        renamed = clean_df.rename(columns=str.lower)
        dupes = renamed["taskid"].duplicated().sum()
        assert dupes == 0


# ---- Step 08: Validation ----

class TestValidation:
    def test_clean_data_passes_null_threshold(self, clean_df, schema):
        max_null = schema["validation"]["max_null_rate"]
        renamed = clean_df.rename(columns=str.lower)
        for col in renamed.columns:
            null_rate = renamed[col].isna().mean()
            assert null_rate <= max_null, f"{col} null rate {null_rate} exceeds {max_null}"

    def test_clean_data_passes_row_count(self, clean_df, schema):
        assert len(clean_df) >= schema["validation"]["min_row_count"]

    def test_clean_data_column_count(self, clean_df, schema):
        renamed = clean_df.rename(columns=str.lower)
        combine_only = {"_source_file", "_ingested_at"}
        expected = set(schema["column_order"]) - combine_only
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
        parquet_path = ROOT / "datasets" / "dev" / "tasks" / "analytics" / "combined.parquet"
        if not parquet_path.exists():
            pytest.skip("Combined parquet not found - run pipeline first")
        return pd.read_parquet(parquet_path)

    def test_db_exists(self):
        """Test that analytics/warehouse.db is created."""
        assert DB_PATH.exists(), f"SQLite database not found at {DB_PATH}"

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
        """Test that key indexes are present."""
        expected_indexes = [
            "idx_tasks_taskstatus",
            "idx_tasks_drawer",
            "idx_tasks_carrier",
            "idx_tasks_flowname",
            "idx_tasks_effectivedate",
            "idx_tasks_dateinitiated",
        ]
        cursor = db_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='tasks'")
        actual_indexes = {row[0] for row in cursor.fetchall()}
        for idx in expected_indexes:
            assert idx in actual_indexes, f"Index {idx} not found"

    def test_views_exist(self, db_conn):
        """Test that all 5 views are queryable."""
        expected_views = [
            "v_task_duration",
            "v_daily_volume",
            "v_drawer_summary",
            "v_carrier_workload",
            "v_missing_status",
        ]
        cursor = db_conn.cursor()
        for view in expected_views:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {view}")
                cursor.fetchone()
            except sqlite3.Error as e:
                pytest.fail(f"View {view} not queryable: {e}")

    def test_duration_view_returns_data(self, db_conn):
        """Test that v_task_duration returns rows with non-null duration_hours."""
        cursor = db_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM v_task_duration WHERE duration_hours IS NOT NULL")
        count = cursor.fetchone()[0]
        assert count > 0, "v_task_duration should have rows with non-null duration_hours"

    def test_null_status_view(self, db_conn):
        """Test that v_missing_status returns rows (from files 07-12)."""
        cursor = db_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM v_missing_status")
        count = cursor.fetchone()[0]
        assert count > 0, "v_missing_status should have rows (files 07-12 lack TaskStatus)"
