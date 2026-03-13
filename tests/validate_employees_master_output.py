"""One-off validation for employees_master combined.parquet (Prompt 9)."""
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
path = ROOT / "datasets/dev/employees_master/analytics/combined.parquet"
df = pd.read_parquet(path)

print("=== Basic Stats ===")
print(f"Total rows: {len(df)}")
print(f"Total columns: {len(df.columns)}")
print(f"Columns: {list(df.columns)}")

print("\n=== Rows per Source ===")
print(df["source_system"].value_counts())

print("\n=== Duplicate Employee IDs ===")
dup_ids = df[df.duplicated(subset=["employee_id"], keep=False)]
print(f"Rows with duplicate employee_id: {len(dup_ids)}")
if len(dup_ids) > 0:
    print("\nSample duplicates:")
    print(dup_ids[["employee_id", "name", "source_system"]].head(10))

print("\n=== Null Rates (%) ===")
null_rates = (df.isnull().sum() / len(df) * 100).round(1)
print(null_rates.to_string())

print("\n=== Sample Rows ===")
print(df[["employee_id", "name", "source_system", "email", "genpact_id"]].head(10))
