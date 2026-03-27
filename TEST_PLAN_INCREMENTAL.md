# Incremental ingestion — manual test plan

Prerequisites: Python env activated, `datasets/dev/tasks/` (or your `--env` / `--dataset`) with `raw/`, `config/`, etc. Use a **copy** of production data or fixtures if you need a safe sandbox.

Paths below assume `datasets/dev/tasks/`; adjust `--env` / `--dataset` as needed.

---

## TP-1 — Full run (cold / first materialization)

**Command**

```bash
python run_pipeline.py --dataset tasks
```

**Expect**

- Step 01 converts every Excel under `raw/` (e.g. 12 files if that is your layout).
- After the run, state exists: `datasets/dev/tasks/_state/ingestion_state.json` (directory may be gitignored; it should still exist on disk).
- `ingestion_state.json` contains `"files"` with one entry per raw workbook name, each with `md5`, `size_bytes`, and timestamps.

**Verify**

```bash
dir datasets\dev\tasks\_state
type datasets\dev\tasks\_state\ingestion_state.json
```

---

## TP-2 — Immediate rerun (no raw changes)

**Command**

```bash
python run_pipeline.py --dataset tasks
```

**Expect**

- Step 01 logs **no** per-file “Converting …” lines for unchanged inputs.
- Log line in `datasets/dev/tasks/logs/pipeline.log` (and console) for step 01:

  `No new or changed files. Skipping convert. (N unchanged)`  

  with `N` equal to your raw Excel count (e.g. 12).

- **Timing:** Step 01 should finish this skip path in **a few seconds** (hashing + fingerprint I/O only). The **full** pipeline may still take longer because steps 02–05 process existing `clean/*.parquet` files unless you add further incremental logic; only step 06 may skip when the step‑01 skip sentinel applies and `combined.parquet` already exists.

**To measure step 01 only (~5 s or less):**

```powershell
$env:PIPELINE_DATASET_ROOT = "C:\path\to\ExcelIngestion\datasets\dev\tasks"
$env:PIPELINE_ENV = "dev"
Measure-Command { python scripts/01_convert.py }
```

---

## TP-3 — Add one new Excel

**Steps**

1. Copy a **new** `.xlsx` into `raw/` (name not in the previous `ingestion_state.json` keys).
2. Run:

   ```bash
   python run_pipeline.py --dataset tasks
   ```

**Expect**

- Step 01 logs **only one** “Converting …” for the new file; others unchanged.
- `combined.parquet` under `analytics/` is rebuilt (new `mtime` / row count includes the new source).
- `ingestion_state.json` contains a new entry for the new filename.

---

## TP-4 — Force full reprocess

**Command**

```bash
python run_pipeline.py --dataset tasks --force
```

**Expect**

- Step 01 logs “Converting …” for **every** raw Excel (fingerprints ignored).
- `PIPELINE_FORCE=1` is set for subprocesses (`run_pipeline`); step 06 does **not** use the “skip combine” shortcut while force is on.

---

## TP-5 — Delete a raw Excel

**Steps**

1. Remove one workbook from `raw/` (do not delete its row from `ingestion_state.json` manually).
2. Run:

   ```bash
   python run_pipeline.py --dataset tasks
   ```

**Expect**

- That filename disappears from `ingestion_state.json` after the run.
- Orphan `clean/<stem>.parquet` for the removed raw file is **deleted** by step 01 (log: `Removed orphan clean Parquet (no matching raw): …`).
- `combined.parquet` is rebuilt **without** rows from the removed source (check row counts or `_source_file` in the combined output).

---

## TP-6 — Delete a `clean/*.parquet` (hash unchanged)

**Steps**

1. Do **not** change the corresponding raw file.
2. Delete one `clean/<stem>.parquet` (not `*_errors.parquet` unless you intend to test that).
3. Run:

   ```bash
   python run_pipeline.py --dataset tasks
   ```

**Expect**

- Step 01 treats that workbook as work again: log “Converting …” for that file, because the expected Parquet is missing even if MD5 matches stored state.
- Parquet is recreated; downstream steps run; combine reflects the dataset.

---

## Quick reference

| Check | Where to look |
|--------|----------------|
| Skip convert | `logs/pipeline.log` — `No new or changed files. Skipping convert.` |
| Force | `python run_pipeline.py --dataset tasks --force` |
| State | `datasets/{env}/{dataset}/_state/ingestion_state.json` |
| Orphan clean | `Removed orphan clean Parquet (no matching raw):` in step 01 logs |
| Skip combine | `Step 01 skipped (no raw changes); combined.parquet exists. Skipping combine.` in step 06 (when sentinel + conditions apply) |

---

## Automated regression (optional)

```bash
pytest tests/ -v
```

Existing tests do not cover incremental ingestion end‑to‑end; use the scenarios above for fingerprint behavior.
