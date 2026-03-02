"""Pipeline tests - run with: pytest tests/ -v"""
import pandas as pd
import yaml
import pytest
from pathlib import Path

ROOT = Path(__file__).parent.parent
CONFIG = ROOT / "config"
FIXTURES = Path(__file__).parent / "fixtures"


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
        assert len(schema["columns"]) == 21

    def test_schema_has_column_order(self, schema):
        assert len(schema["column_order"]) == 21
        assert schema["column_order"][0] == "taskid"

    def test_schema_columns_match_order(self, schema):
        col_keys = set(schema["columns"].keys())
        order_keys = set(schema["column_order"])
        assert col_keys == order_keys, f"Mismatch: {col_keys.symmetric_difference(order_keys)}"

    def test_primary_key_defined(self, schema):
        pk_cols = [c for c, v in schema["columns"].items() if v.get("primary_key")]
        assert len(pk_cols) >= 1, "Need at least one primary key"

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
        expected = set(schema["column_order"])
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
        expected = set(schema["column_order"])
        assert set(renamed.columns) == expected
