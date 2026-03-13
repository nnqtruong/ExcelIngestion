-- Fail if row count of source tasks Parquet does not match mart_tasks_enriched (no row loss).
-- Returns rows when counts differ so the test fails.
select
  1 as row_loss_detected
where (select count(*) from '../datasets/dev/tasks/analytics/combined.parquet') != (select count(*) from "dev_warehouse"."main"."mart_tasks_enriched")