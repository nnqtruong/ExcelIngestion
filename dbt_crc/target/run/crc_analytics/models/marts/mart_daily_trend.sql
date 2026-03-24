
  
    
    

    create  table
      "dev_warehouse"."main"."mart_daily_trend__dbt_tmp"
  
    as (
      

select
  cast(t.dateinitiated as date) as task_date,
  t.drawer,
  count(*) as tasks_opened,
  sum(case when t.taskstatus = 'Completed' then 1 else 0 end) as tasks_completed,
  count(*) - sum(case when t.taskstatus = 'Completed' then 1 else 0 end) as net_backlog_change
from "dev_warehouse"."main"."stg_tasks" t
group by cast(t.dateinitiated as date), t.drawer
    );
  
  