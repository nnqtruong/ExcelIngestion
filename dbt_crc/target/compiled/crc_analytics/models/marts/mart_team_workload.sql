
select
  assigned_team,
  assigned_division,
  count(*) as task_count,
  round(avg(lifecycle_hours), 2) as avg_lifecycle_hours,
  sum(case when taskstatus is null or taskstatus != 'Completed' then 1 else 0 end) as open_tasks
from "dev_warehouse"."main"."mart_tasks_enriched"
where assigned_team is not null and assigned_division is not null
group by assigned_team, assigned_division
order by task_count desc