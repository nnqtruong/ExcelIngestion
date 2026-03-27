# SQLite Performance Optimization — Build Manifesto

## End Goal

Step 10 (SQLite views) completes in under 30 seconds for the full pipeline run, including verification. Currently it takes 10+ minutes because every mart view re-executes expensive JOINs across 4.7M task rows. The solution: join once into a materialized staging table, then all 7 mart views read from that flat indexed table.

## Current State Assessment

### Architecture
- **Pipeline**: 10 steps processing 4 datasets (tasks, dept_mapping, workers, employees_master)
- **Step 09**: Exports combined.parquet to SQLite tables (tasks: 4.7M rows, workers: ~4K rows, employees_master: ~3K rows)
- **Step 10**: Creates 7 mart views by translating dbt SQL (DuckDB dialect) to SQLite

### Current Files
| File | Purpose |
|------|---------|
| `lib/export_sqlite.py` | Step 09: Writes tables with basic indexes (taskstatus, taskid, dateinitiated, assignedto, operationby) |
| `lib/sqlite_views.py` | Step 10: Orchestrates view creation, verification (currently LIMIT 1 only) |
| `lib/sync_mart_views_sqlite.py` | Translates dbt mart SQL from DuckDB to SQLite dialect |
| `dbt_crc/models/marts/*.sql` | 7 mart definitions (source of truth for both DuckDB and SQLite) |
| `dbt_crc/models/staging/stg_tasks.sql` | Staging view with value map joins and normalized columns |

### Current View Creation Flow
```
Step 10 (sqlite_views.py):
  1. Drop legacy v_* views
  2. Load dbt seed CSVs (value_map_taskstatus, etc.) as SQLite tables
  3. Create stg_tasks view (translates dbt staging SQL)
  4. For each mart SQL file:
     a. Read dbt SQL
     b. Translate DuckDB syntax to SQLite (sync_mart_views_sqlite.py)
     c. CREATE VIEW as the translated SELECT
  5. Verify each view with SELECT 1 ... LIMIT 1
```

### Performance Problem
The marts that join tasks to workers/employees_master are slow because:
1. **SQLite views are not cached** — the full JOIN executes every time a view is queried
2. **Join conditions use LOWER(TRIM(CAST(...)))** — prevents index usage
3. **Same 4.7M row JOIN repeated** — mart_tasks_enriched, mart_team_demand, mart_onshore_offshore all re-execute the same join
4. **Verification currently uses LIMIT 1** — even that is slow for complex joins

### Current Join Pattern (from dbt marts)
```sql
-- mart_tasks_enriched and mart_team_demand use:
ON t.assignedto = lower(trim(cast(w.employee_id as varchar)))

-- mart_onshore_offshore uses:
ON t.assignedto = em.employee_id
```

Note: `stg_tasks` already normalizes `assignedto` with `trim(lower(t.assignedto))`, so the tasks side is already clean. The problem is the workers/employees_master side re-computing `lower(trim(cast(...)))` for every row.

## Proposed Architecture

### BEFORE (slow):
```
mart_tasks_enriched  →  stg_tasks + workers + employees_master JOIN (4.7M × 4K × 3K)
mart_team_demand     →  stg_tasks + workers JOIN (4.7M × 4K)  ← same join again
mart_onshore_offshore → stg_tasks + employees_master JOIN (4.7M × 3K) ← same join again
= 3+ full joins on 4.7M rows = 10+ minutes
```

### AFTER (fast):
```
Step 10: CREATE TABLE _stg_tasks_enriched AS (one JOIN, one time)
Step 10: CREATE INDEXES on _stg_tasks_enriched
mart_tasks_enriched  →  SELECT * FROM _stg_tasks_enriched (instant)
mart_team_demand     →  GROUP BY on _stg_tasks_enriched (seconds)
mart_onshore_offshore → GROUP BY on _stg_tasks_enriched (seconds)
mart_turnaround      →  GROUP BY on _stg_tasks_enriched (seconds)
mart_team_capacity   →  GROUP BY on workers directly (no tasks join, already fast)
mart_backlog         →  GROUP BY on _stg_tasks_enriched (seconds)
mart_daily_trend     →  GROUP BY on _stg_tasks_enriched (seconds)
= 1 join + 6 indexed scans = under 30 seconds
```

## Build Phase

### Step 1: Modify `lib/sqlite_views.py` to create materialized staging table

Add a new function `create_materialized_staging_table()` that runs AFTER seed tables are loaded but BEFORE mart views are created:

```sql
DROP TABLE IF EXISTS _stg_tasks_enriched;

CREATE TABLE _stg_tasks_enriched AS
SELECT
    t.*,
    w.teammate,
    w.job_profile AS worker_job_profile,
    w.business_title,
    w.management_level,
    w.cost_center,
    w.cost_center_hierarchy,
    w.fte,
    w.scheduled_weekly_hours,
    w.direct_manager,
    w.current_status AS worker_status,
    em.source_system AS employee_source,
    em.name AS employee_master_name,
    (julianday(t.endtime) - julianday(t.starttime)) * 1440 AS duration_minutes,
    ROUND((julianday(t.endtime) - julianday(t.starttime)) * 24, 2) AS duration_hours,
    ROUND((julianday(t.dateended) - julianday(t.dateinitiated)) * 24, 2) AS lifecycle_hours,
    DATE(t.dateinitiated) AS task_date
FROM stg_tasks t
LEFT JOIN workers w
    ON t.assignedto = LOWER(TRIM(CAST(w.employee_id AS TEXT)))
LEFT JOIN employees_master em
    ON t.assignedto = LOWER(TRIM(CAST(em.employee_id AS TEXT)));
```

This is the ONLY time the expensive join runs. Log time and row count.

### Step 2: Index the staging table

```sql
CREATE INDEX idx_stg_task_date ON _stg_tasks_enriched(task_date);
CREATE INDEX idx_stg_drawer ON _stg_tasks_enriched(drawer);
CREATE INDEX idx_stg_taskstatus ON _stg_tasks_enriched(taskstatus);
CREATE INDEX idx_stg_flowname ON _stg_tasks_enriched(flowname);
CREATE INDEX idx_stg_cost_center ON _stg_tasks_enriched(cost_center_hierarchy);
CREATE INDEX idx_stg_source ON _stg_tasks_enriched(employee_source);
CREATE INDEX idx_stg_stepname ON _stg_tasks_enriched(stepname);
```

### Step 3: Create SQLite-specific mart view definitions

Instead of translating dbt SQL (which re-introduces the joins), create SQLite-specific view definitions that read from `_stg_tasks_enriched`.

Add to `lib/sqlite_views.py`:

```python
# SQLite-specific mart view definitions (use materialized staging table)
SQLITE_MART_VIEWS = {
    "mart_tasks_enriched": """
        SELECT * FROM _stg_tasks_enriched
    """,

    "mart_team_capacity": """
        SELECT
            cost_center_hierarchy AS department,
            cost_center,
            management_level,
            COUNT(*) AS headcount,
            SUM(fte) AS total_fte,
            SUM(scheduled_weekly_hours) AS total_weekly_hours
        FROM workers
        WHERE current_status = 'Active'
        GROUP BY cost_center_hierarchy, cost_center, management_level
    """,

    "mart_team_demand": """
        SELECT
            cost_center_hierarchy AS department,
            cost_center,
            task_date AS task_week,
            COUNT(*) AS task_count,
            SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS completed,
            ROUND(AVG(duration_hours), 2) AS avg_handle_hours
        FROM _stg_tasks_enriched
        WHERE duration_hours IS NOT NULL
        GROUP BY cost_center_hierarchy, cost_center, task_date
    """,

    "mart_onshore_offshore": """
        SELECT
            employee_source AS source_system,
            flowname,
            stepname,
            COUNT(*) AS task_count,
            ROUND(AVG(duration_hours), 2) AS avg_handle_hours,
            ROUND(
                SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                1
            ) AS completion_rate
        FROM _stg_tasks_enriched
        WHERE duration_hours IS NOT NULL
        GROUP BY employee_source, flowname, stepname
    """,

    "mart_backlog": """
        SELECT
            drawer,
            flowname,
            stepname,
            taskstatus,
            COUNT(*) AS task_count,
            ROUND(AVG(julianday('now') - julianday(dateinitiated)), 1) AS avg_age_days
        FROM _stg_tasks_enriched
        WHERE taskstatus IS NOT NULL AND taskstatus != 'Completed'
        GROUP BY drawer, flowname, stepname, taskstatus
    """,

    "mart_turnaround": """
        SELECT
            drawer,
            flowname,
            stepname,
            COUNT(*) AS completed_count,
            ROUND(AVG(duration_hours), 2) AS avg_handle_hours,
            ROUND(AVG(lifecycle_hours), 2) AS avg_lifecycle_hours
        FROM _stg_tasks_enriched
        WHERE taskstatus = 'Completed'
            AND duration_hours IS NOT NULL
        GROUP BY drawer, flowname, stepname
    """,

    "mart_daily_trend": """
        SELECT
            task_date,
            drawer,
            COUNT(*) AS tasks_opened,
            SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS tasks_completed,
            COUNT(*) - SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS net_backlog_change
        FROM _stg_tasks_enriched
        GROUP BY task_date, drawer
    """,
}
```

### Step 4: Update `run_sqlite_views()` flow

```python
@monitor_step
def run_sqlite_views(db_path: Path, log: logging.Logger) -> dict:
    """Drop legacy views, create materialized staging, create mart views, verify."""
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        _drop_legacy_views(conn, log)
    finally:
        conn.close()

    # Load seed tables (value maps)
    sync_seed_tables(db_path, SEEDS_DIR, log)

    # Create stg_tasks view (needed by materialized staging table)
    if STAGING_DIR.is_dir():
        staging_created, staging_errors = sync_staging_views(db_path, STAGING_DIR, log)
        for err in staging_errors:
            log.warning("Staging view sync: %s", err)

    # NEW: Create materialized staging table (the one expensive join)
    create_materialized_staging_table(db_path, log)

    # Create SQLite-specific mart views (read from _stg_tasks_enriched)
    created = create_sqlite_mart_views(db_path, log)

    # Verify with COUNT(*) — should be fast now with indexed flat table
    conn = sqlite3.connect(db_path)
    try:
        return verify_views(conn, created, log)
    finally:
        conn.close()
```

### Step 5: Update view verification back to COUNT(*)

```python
def verify_views(conn: sqlite3.Connection, view_names: list[str], log: logging.Logger) -> dict:
    """Verify views with COUNT(*) — fast now that views read from indexed flat table."""
    cursor = conn.cursor()
    results: dict[str, int] = {}
    for view_name in view_names:
        try:
            start = time.time()
            cursor.execute(f'SELECT COUNT(*) FROM "{view_name}"')
            count = cursor.fetchone()[0]
            elapsed = time.time() - start
            results[view_name] = count
            log.info("View %s: %d rows (%.1fs)", view_name, count, elapsed)
        except sqlite3.Error as e:
            log.warning("Failed to query view %s: %s", view_name, e)
            results[view_name] = -1
    return results
```

### Step 6: Handle missing tables gracefully

Before creating `_stg_tasks_enriched`, check which tables exist:

```python
def create_materialized_staging_table(db_path: Path, log: logging.Logger) -> None:
    """Create the pre-joined staging table. Handles missing optional tables."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check which tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing_tables = {row[0] for row in cursor.fetchall()}

    # stg_tasks view must exist (created from tasks table + value maps)
    if "stg_tasks" not in existing_tables and "tasks" not in existing_tables:
        log.warning("tasks table not found; skipping materialized staging table")
        conn.close()
        return

    has_workers = "workers" in existing_tables
    has_employees_master = "employees_master" in existing_tables

    log.info("Creating _stg_tasks_enriched (workers=%s, employees_master=%s)",
             has_workers, has_employees_master)

    # Build SQL dynamically based on available tables
    # ... (build appropriate SELECT with optional JOINs)

    start = time.time()
    cursor.executescript(create_sql)
    conn.commit()
    elapsed = time.time() - start

    # Verify row count
    cursor.execute("SELECT COUNT(*) FROM _stg_tasks_enriched")
    count = cursor.fetchone()[0]
    log.info("Created _stg_tasks_enriched: %d rows in %.1fs", count, elapsed)

    # Create indexes
    # ...

    conn.close()
```

## Build Rules

1. **Only modify `lib/sqlite_views.py`** — do NOT change steps 01-09 or dbt mart SQL files
2. **The `_stg_tasks_enriched` table is internal** (prefixed with underscore) — not user-facing
3. **Keep dbt marts unchanged** — they remain the source of truth for DuckDB; SQLite uses its own optimized definitions
4. **Log timing** for the staging table creation separately so we can track performance
5. **All existing tests must still pass**
6. **Views must produce identical results** — same columns, same row counts as current views

## Success Criteria

- [ ] Step 10 completes in under 30 seconds (currently 10+ minutes)
- [ ] `_stg_tasks_enriched` table exists with row count matching tasks table
- [ ] All 7 mart views exist and return data
- [ ] View verification (COUNT(*)) completes in under 5 seconds per view
- [ ] Pipeline runs with `--all` without hanging
- [ ] Views produce same results as before (spot check mart_turnaround and mart_daily_trend row counts)
- [ ] Graceful handling when workers or employees_master tables are missing
- [ ] All existing tests pass

## Files to Modify

| File | Changes |
|------|---------|
| `lib/sqlite_views.py` | Add `create_materialized_staging_table()`, `SQLITE_MART_VIEWS`, update `run_sqlite_views()`, restore COUNT(*) verification |

## Files NOT to Modify

- `lib/export_sqlite.py` — keep step 09 unchanged
- `lib/sync_mart_views_sqlite.py` — keep translation logic for reference
- `dbt_crc/models/marts/*.sql` — dbt remains source of truth for DuckDB
- `scripts/09_export_sqlite.py` — keep unchanged
- `scripts/10_sqlite_views.py` — keep unchanged (just calls lib function)
