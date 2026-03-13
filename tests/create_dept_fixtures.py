"""
Generate one Excel fixture for dept_mapping pipeline: employee_mapping.xlsx with 200 rows
of realistic CRC employee mapping data and injected data quality issues.

Run:
  python tests/create_dept_fixtures.py                              # dev (default)
  python tests/create_dept_fixtures.py --output-dir datasets/dev/dept_mapping/raw
  python tests/create_dept_fixtures.py --output-dir datasets/prod/dept_mapping/raw
"""
import argparse
import random
import sys
from pathlib import Path

import pandas as pd

random.seed(42)

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = ROOT / "datasets" / "dev" / "dept_mapping" / "raw"

# Column names exactly as source (ID has trailing space in header)
COLUMNS = [
    "userid",
    "ID ",  # trailing space required
    "Full Name",
    "title",
    "netwarelogin",
    "email",
    "divisionid",
    "Division",
    "Division1",
    "teamid",
    "Team",
]

DIVISIONS = [
    ("CRCAL1", "CRC - Birmingham"),
    ("CRCDL", "CRC - Dallas (Tollway)"),
    ("CWHS", "CRC - Woodland Hills"),
    ("FWTB", "SCU - Fort Worth"),
    ("CCHI", "CRC/SWC - Chicago"),
    ("CTAM", "SCU - Tampa"),
    ("CDEN", "CRC - Denver"),
    ("CATL", "CRC - Atlanta"),
    ("CNYC", "CRC - New York"),
    ("CPOR", "CRC - Portland"),
]

TITLES = [
    "Broker",
    "Senior Broker",
    "Associate Broker",
    "Inside Underwriter",
    "Underwriter",
    "Account Executive",
    "Senior Account Executive",
    "Operations Specialist",
    "Claims Analyst",
]

# Pools for generating 198+ unique (first_initial + lastname) userids
FIRST_NAMES = [
    "Aaron", "Beth", "Carl", "Diana", "Evan", "Fiona", "Greg", "Helen",
    "Ivan", "Julia", "Kevin", "Laura", "Mark", "Nancy", "Oscar", "Paula",
    "Quinn", "Rachel", "Steve", "Tina", "Uma", "Victor", "Wendy", "Xavier",
    "Yvonne", "Zach", "Alice", "Bob", "Claire", "David", "Emma", "Frank",
    "Grace", "Henry", "Ivy", "Jack", "Kate", "Leo", "Maya", "Noah",
]
LAST_NAMES = [
    "Blain", "Parks", "Chen", "Torres", "Nguyen", "Reeves", "Coleman",
    "Daugherty", "Willson", "Rykard", "Blackwell", "Smith", "Jones",
    "Brown", "Davis", "Wilson", "Taylor", "Moore", "Anderson", "Thomas",
    "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez",
    "Robinson", "Clark", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young",
    "King", "Wright", "Scott", "Green", "Baker", "Adams", "Nelson", "Hill",
    "Campbell", "Mitchell", "Roberts", "Carter", "Phillips", "Evans", "Turner",
]


def userid_from_name(first: str, last: str) -> str:
    """First initial + last name, lowercase. e.g. Aaron Blain -> ablain."""
    return (first[0] + last).lower()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate employee_mapping.xlsx fixture for dept_mapping pipeline.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_RAW_DIR,
        help=f"Directory to write Excel file (default: {DEFAULT_RAW_DIR})",
    )
    args = parser.parse_args()

    raw_dir = args.output_dir.resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_path = raw_dir / "employee_mapping.xlsx"

    # Build 198 unique people, then add 2 duplicate userid rows (200 total)
    seen_userids: set[str] = set()
    rows: list[dict] = []
    first_last_pairs: list[tuple[str, str]] = []
    for first in FIRST_NAMES:
        for last in LAST_NAMES:
            uid = userid_from_name(first, last)
            if uid not in seen_userids:
                seen_userids.add(uid)
                first_last_pairs.append((first, last))
            if len(first_last_pairs) >= 200:
                break
        if len(first_last_pairs) >= 200:
            break
    # Trim to 198 for unique rows, then we'll add 2 duplicates
    first_last_pairs = first_last_pairs[:198]

    for first, last in first_last_pairs:
        div_id, div_name = random.choice(DIVISIONS)
        title = random.choice(TITLES)
        uid = userid_from_name(first, last)
        full_name = f"{first} {last}"
        # ID: same as userid but sometimes different casing
        id_val = random.choice([uid, uid.capitalize(), uid.title()])
        netwarelogin = f"CRC\\{uid}"
        email = f"{uid}@crcgroup.com"
        team_id = str(random.randint(1, 50))
        team = f"{div_id}-Team {last}"
        rows.append({
            "userid": uid,
            "ID ": id_val,
            "Full Name": full_name,
            "title": title,
            "netwarelogin": netwarelogin,
            "email": email,
            "divisionid": div_id,
            "Division": div_name,
            "Division1": "CRC",
            "teamid": team_id,
            "Team": team,
        })

    # Add 2 duplicate userid rows (same userid as row 0 and row 1, different other data)
    for i in (0, 1):
        src = rows[i]
        div_id, div_name = random.choice(DIVISIONS)
        title = random.choice(TITLES)
        last = src["Team"].split("Team ")[-1]
        rows.append({
            "userid": src["userid"],
            "ID ": src["ID "],
            "Full Name": src["Full Name"],
            "title": title,
            "netwarelogin": src["netwarelogin"],
            "email": src["email"],
            "divisionid": div_id,
            "Division": div_name,
            "Division1": "CRC",
            "teamid": str(random.randint(1, 50)),
            "Team": f"{div_id}-Team {last}",
        })

    df = pd.DataFrame(rows, columns=COLUMNS)

    # --- Inject data quality issues (disjoint row indices) ---
    n = len(df)
    indices = list(range(n))
    random.shuffle(indices)
    pos = 0

    def take(k: int) -> list[int]:
        nonlocal pos
        out = indices[pos : pos + k]
        pos += k
        return out

    # 10 rows: literal string NULL in title
    for i in take(10):
        df.at[i, "title"] = "NULL"

    # 8 rows: lowercase crc\ in netwarelogin
    for i in take(8):
        uid = df.at[i, "userid"]
        df.at[i, "netwarelogin"] = f"crc\\{uid}"

    # 3 rows: Crc\ in netwarelogin
    for i in take(3):
        uid = df.at[i, "userid"]
        df.at[i, "netwarelogin"] = f"Crc\\{uid}"

    # 5 rows: trailing whitespace in Full Name
    for i in take(5):
        df.at[i, "Full Name"] = df.at[i, "Full Name"] + "   "

    # 3 rows: empty string in title
    for i in take(3):
        df.at[i, "title"] = ""

    # 1 row: email missing @crcgroup.com (just userid)
    for i in take(1):
        df.at[i, "email"] = df.at[i, "userid"]

    # 2 duplicate userids are already in the data (last 2 rows)

    dirt_counts = {
        "title literal NULL": 10,
        "netwarelogin crc\\ (lowercase)": 8,
        "netwarelogin Crc\\": 3,
        "Full Name trailing whitespace": 5,
        "duplicate userids": 2,  # 2 userids each appear twice
        "title empty string": 3,
        "email missing @crcgroup.com": 1,
    }

    # Write Excel (column "ID " must have trailing space in header)
    df.to_excel(out_path, index=False, sheet_name="Sheet1", engine="openpyxl")

    # Summary
    print("Generated:", out_path)
    print("Total rows:", len(df))
    print("Columns:", list(df.columns))
    print("Column 'ID ' has trailing space:", df.columns[1] == "ID ")
    print("\nDirt injected:")
    for label, count in dirt_counts.items():
        print(f"  {label}: {count}")

    # Verify read-back
    df2 = pd.read_excel(out_path, sheet_name="Sheet1", engine="openpyxl")
    assert list(df2.columns) == list(df.columns), "Column names mismatch on read"
    assert len(df2) == 200, f"Expected 200 rows, got {len(df2)}"
    # Check duplicate userids
    dup = df2["userid"].duplicated(keep=False)
    dup_userids = df2.loc[dup, "userid"].unique()
    assert len(dup_userids) == 2, f"Expected 2 duplicated userids, got {len(dup_userids)}"
    print("\nRead-back OK: 200 rows, columns match, 2 duplicated userids verified.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
