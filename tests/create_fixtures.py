"""
Generate 12 realistic test Excel files for pipeline testing.
- Files 01-06: full columns (all 21)
- Files 07-12: missing TaskStatus column

Run: python tests/create_fixtures.py
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import random

random.seed(42)
np.random.seed(42)

RAW_DIR = Path(__file__).parent.parent / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# --- Reference data pools ---
DRAWERS = [
    "SCU Bothell", "San Fran TC Brkg", "SCU Tampa",
    "SCU Arizona Daugherty", "SCU Dallas", "SCU Chicago",
    "SCU Denver", "SCU Atlanta", "NYC Brkg", "SCU Portland",
]

CARRIERS = [
    "Western World Insurance Company", "Markel",
    "Scottsdale Insurance Company", "Markel West - Scottsdale",
    "Zurich North America", "Berkshire Hathaway Specialty",
    "Chubb", "Hartford Financial Services", "Travelers",
]

ACCT_EXECS = [
    "Minyette Willson", "Lee Coleman", "James Rykard",
    "Abby Daugherty", "Sarah Chen", "Mike Torres",
    "Dana Reeves", "Tom Blackwell", "Lisa Nguyen",
]

FLOWNAMES = ["UW Renewal", "UW New Business", "Brokerage Support Processing"]

STEPNAMES = [
    "Policy Issuance", "Order Loss Runs", "Ren - Begin Renewal",
    "Quote Review", "Bind Request", "Endorsement Processing",
]

SENTTO = [
    "Task Complete", "LR - Loss Runs Follow Up",
    "Ren - Quote to Agent", "Pending Review", "Awaiting Docs",
]

STATUSES = ["Completed", "Completed", "Completed", "In Progress", "Pending"]

TASK_DESCRIPTIONS = [
    "Western World", "FU Midterm Loss run Request - Markel - 24-26",
    "DID QTE BIND?", "Renewal follow up", "Endorsement request",
    "Quote comparison needed", "Bind confirmation pending",
]


def random_ts(base_date, offset_hours=0):
    """Generate a realistic timestamp."""
    dt = base_date + timedelta(
        hours=offset_hours + random.randint(0, 8),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
        microseconds=random.randint(0, 999999),
    )
    return dt


def generate_rows(n_rows, start_taskid):
    """Generate n_rows of realistic CRC task data."""
    rows = []
    for i in range(n_rows):
        taskid = start_taskid + i
        base_date = datetime(2025, random.randint(7, 12), random.randint(1, 28))
        effective_date = base_date + timedelta(days=random.randint(0, 60))
        date_initiated = random_ts(base_date)
        date_available = random_ts(base_date, offset_hours=random.randint(1, 48))
        date_ended = random_ts(base_date, offset_hours=random.randint(24, 720))
        start_time = random_ts(base_date, offset_hours=random.randint(0, 4))
        end_time = random_ts(base_date, offset_hours=random.randint(24, 720))

        filenumber = 14000000 + random.randint(100000, 300000)
        policy_prefix = random.choice(["NPP", "MKLV", "CPS", "EZXS", "ZNA", "BHS"])
        policy_number = f"{policy_prefix}{random.randint(1000000, 9999999)}"
        assigned_to = random.choice(["OpsCentral", "BrokerageLossRuns", "jrykard", "303200", "304486"])
        task_from = random.choice(["dpulver", "304486", "IRServices", "303200", "DeeBabu"])
        operation_by = random.choice(["303363", "DeeBabu", "JSuskind", "303200", "304486"])

        row = {
            "taskid": taskid,
            "Drawer": random.choice(DRAWERS),
            "PolicyNumber": policy_number,
            "filename": f"Test Client {taskid % 1000}, Inc",
            "filenumber": filenumber,
            "EffectiveDate": effective_date.strftime("%Y-%m-%d %H:%M:%S"),
            "Carrier": random.choice(CARRIERS),
            "AcctExec": random.choice(ACCT_EXECS),
            "TaskDescription": random.choice(TASK_DESCRIPTIONS),
            "AssignedTo": assigned_to,
            "TaskFrom": task_from,
            "OperationBy": operation_by,
            "flowname": random.choice(FLOWNAMES),
            "stepname": random.choice(STEPNAMES),
            "SentTo": random.choice(SENTTO),
            "dateavailable": date_available.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "dateinitiated": date_initiated.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "dateended": date_ended.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "TaskStatus": random.choice(STATUSES),
            "starttime": start_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "endtime": end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
        }
        rows.append(row)
    return rows


def inject_realistic_dirt(df, file_index):
    """Add realistic data quality issues."""
    n = len(df)

    # Sprinkle some nulls in non-critical fields
    for col in ["AcctExec", "filename", "SentTo"]:
        if col in df.columns:
            null_idx = np.random.choice(n, size=max(1, n // 20), replace=False)
            df.loc[null_idx, col] = None

    # Occasional bad casing in flowname
    if file_index % 3 == 0:
        bad_idx = np.random.choice(n, size=max(1, n // 15), replace=False)
        df.loc[bad_idx, "flowname"] = df.loc[bad_idx, "flowname"].str.lower()

    # Occasional bad casing in TaskStatus
    if "TaskStatus" in df.columns and file_index % 4 == 0:
        bad_idx = np.random.choice(n, size=max(1, n // 15), replace=False)
        df.loc[bad_idx, "TaskStatus"] = df.loc[bad_idx, "TaskStatus"].str.upper()

    # Double spaces in Drawer sometimes
    if file_index % 5 == 0:
        bad_idx = np.random.choice(n, size=max(1, n // 20), replace=False)
        df.loc[bad_idx, "Drawer"] = df.loc[bad_idx, "Drawer"].str.replace(" ", "  ", n=1)

    return df


# --- Generate 12 files ---
for i in range(1, 13):
    n_rows = random.randint(80, 150)  # realistic batch sizes
    start_id = 40000000 + (i * 100000)
    rows = generate_rows(n_rows, start_id)
    df = pd.DataFrame(rows)

    # Files 7-12: drop TaskStatus column
    if i >= 7:
        df = df.drop(columns=["TaskStatus"])

    # Add realistic dirt
    df = inject_realistic_dirt(df, i)

    filename = f"tasks_batch_{i:02d}.xlsx"
    df.to_excel(RAW_DIR / filename, index=False)

    status_col = "YES" if "TaskStatus" in df.columns else "MISSING"
    print(f"  {filename}  |  {n_rows:>3} rows  |  {len(df.columns):>2} cols  |  TaskStatus: {status_col}")

print(f"\n12 files created in {RAW_DIR.resolve()}")
print(f"  Files 01-06: full columns (21)")
print(f"  Files 07-12: missing TaskStatus column (20)")
