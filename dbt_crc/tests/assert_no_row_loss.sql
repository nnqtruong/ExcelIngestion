-- Fail if row count of source tasks Parquet does not match mart_tasks_enriched (no row loss).
-- Returns rows when counts differ so the test fails.
select
  1 as row_loss_detected
where (select count(*) from {{ source('raw', 'tasks') }}) != (select count(*) from {{ ref('mart_tasks_enriched') }})
