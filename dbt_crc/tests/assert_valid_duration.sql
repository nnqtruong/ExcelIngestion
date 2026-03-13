-- Fail if any row has invalid duration: negative duration_hours or endtime < starttime.
-- Returns violating rows so the test fails.
select
  row_id,
  'negative_duration_hours' as violation
from {{ ref('mart_tasks_enriched') }}
where duration_hours is not null and duration_hours < 0

union all

select
  row_id,
  'endtime_before_starttime' as violation
from {{ ref('mart_tasks_enriched') }}
where starttime is not null and endtime is not null and endtime < starttime
