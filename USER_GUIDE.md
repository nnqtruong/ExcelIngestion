# Excel to Power BI Pipeline - User Guide

This tool converts your Excel files into a format Power BI can read quickly.

---

## What You Need Before Starting

1. **Python installed** (version 3.10 or newer)
   - Download from: https://www.python.org/downloads/
   - During install, check "Add Python to PATH"

2. **DuckDB ODBC Driver** (for Power BI connection)
   - Download from: https://duckdb.org/docs/api/odbc/overview
   - Run the installer

---

## First-Time Setup (Do This Once)

### Step 1: Open Command Prompt

Press `Win + R`, type `cmd`, press Enter.

### Step 2: Go to the Project Folder

```
cd "C:\Users\quang\CRC Code\ExcelIngestion"
```

### Step 3: Create a Virtual Environment

```
python -m venv .venv
```

### Step 4: Activate the Virtual Environment

```
.venv\Scripts\activate
```

You should see `(.venv)` at the start of the command line.

### Step 5: Install Required Packages

```
pip install -r requirements.txt
```

Wait for it to finish (may take 1-2 minutes).

---

## Adding Your Excel Files

1. Open File Explorer
2. Go to: `C:\Users\quang\CRC Code\ExcelIngestion\datasets\tasks\raw`
3. Copy your Excel files (`.xlsx`) into this folder

For employee/department data:
- Go to: `C:\Users\quang\CRC Code\ExcelIngestion\datasets\dept_mapping\raw`
- Copy your employee mapping Excel file here

---

## Running the Pipeline

### Step 1: Open Command Prompt and Activate

```
cd "C:\Users\quang\CRC Code\ExcelIngestion"
.venv\Scripts\activate
```

### Step 2: Run the Pipeline

For task data:
```
python run_pipeline.py
```

For employee data:
```
python run_pipeline.py --dataset dept_mapping
```

Wait for it to finish. You'll see progress messages.

### Step 3: Create the Power BI Database

```
python powerbi/create_duckdb.py
```

You should see:
```
Loaded tasks: 6,000,000 rows
Loaded employees: 200 rows
Loaded tasks_with_dept: 6,000,000 rows
```

---

## Connecting Power BI

### Step 1: Open Power BI Desktop

### Step 2: Get Data

1. Click **Get Data** (Home tab)
2. Click **More...**
3. Search for **ODBC**
4. Click **ODBC**, then **Connect**

### Step 3: Enter Connection String

1. Click **Advanced options**
2. In the **Connection string** box, paste:

```
Driver={DuckDB Driver};Database=C:\Users\quang\CRC Code\ExcelIngestion\powerbi\warehouse.duckdb;access_mode=READ_ONLY
```

3. Click **OK**

### Step 4: Select Your Tables

You'll see these tables:
- `tasks` - All your task data
- `employees` - Employee/department info
- `tasks_with_dept` - Tasks joined with employee info (recommended)

And these views (pre-calculated summaries):
- `v_daily_volume` - Task counts by day
- `v_drawer_summary` - Task counts by drawer
- `v_carrier_workload` - Tasks by carrier

Select the tables you need, click **Load**.

---

## Updating Your Data

When you have new Excel files:

1. Delete old files from the `raw` folder (or add new ones alongside)
2. Run the pipeline again:
   ```
   cd "C:\Users\quang\CRC Code\ExcelIngestion"
   .venv\Scripts\activate
   python run_pipeline.py
   python powerbi/create_duckdb.py
   ```
3. In Power BI, click **Refresh**

---

## Quick Reference

| Task | Command |
|------|---------|
| Activate environment | `.venv\Scripts\activate` |
| Run task pipeline | `python run_pipeline.py` |
| Run employee pipeline | `python run_pipeline.py --dataset dept_mapping` |
| Create Power BI database | `python powerbi/create_duckdb.py` |
| Test ODBC connection | `python powerbi/setup_odbc.py` |

---

## Troubleshooting

### "python is not recognized"
- Reinstall Python and check "Add Python to PATH"
- Or use full path: `C:\Python310\python.exe`

### "No module named pandas"
- Make sure you activated the environment: `.venv\Scripts\activate`
- Reinstall packages: `pip install -r requirements.txt`

### Power BI shows no tables
- Make sure you ran `python powerbi/create_duckdb.py`
- Check the connection string has the correct path
- Try running `python powerbi/setup_odbc.py` to test

### "DuckDB Driver not found"
- Install the DuckDB ODBC driver from https://duckdb.org/docs/api/odbc/overview
- Restart your computer after installing

### Pipeline fails with error
- Check your Excel files are in the correct `raw` folder
- Make sure Excel files are not open in another program
- Try running with `--dry-run` first: `python run_pipeline.py --dry-run`

---

## File Locations

| What | Where |
|------|-------|
| Your Excel files (tasks) | `datasets\tasks\raw\` |
| Your Excel files (employees) | `datasets\dept_mapping\raw\` |
| Power BI database | `powerbi\warehouse.duckdb` |
| Pipeline logs | `datasets\tasks\logs\pipeline.log` |

---

## Getting Help

If you run into issues:
1. Check the Troubleshooting section above
2. Look at the log file: `datasets\tasks\logs\pipeline.log`
3. Contact your system administrator
