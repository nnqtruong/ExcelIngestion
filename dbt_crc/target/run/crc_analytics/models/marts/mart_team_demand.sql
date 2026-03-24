
  
    
    

    create  table
      "dev_warehouse"."main"."mart_team_demand__dbt_tmp"
  
    as (
      

select
  w.cost_center_hierarchy as department,
  w.cost_center,
  cast(t.dateinitiated as date) as task_week,
  count(*) as task_count,
  sum(case when t.taskstatus = 'Completed' then 1 else 0 end) as completed,
  round(avg(datediff('minute', t.starttime, t.endtime) / 60.0), 2) as avg_handle_hours
from "dev_warehouse"."main"."stg_tasks" t
left join "dev_warehouse"."main"."stg_workers" w
  on t.assignedto = lower(trim(cast(w.employee_id as varchar)))
where t.starttime is not null and t.endtime is not null
group by w.cost_center_hierarchy, w.cost_center, cast(t.dateinitiated as date)
    );
  
  