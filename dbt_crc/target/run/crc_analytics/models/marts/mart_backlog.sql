
  
    
    

    create  table
      "dev_warehouse"."main"."mart_backlog__dbt_tmp"
  
    as (
      

select
  t.drawer,
  t.flowname,
  t.stepname,
  t.taskstatus,
  count(*) as task_count,
  round(avg(datediff('day', t.dateinitiated, current_date)), 1) as avg_age_days
from "dev_warehouse"."main"."stg_tasks" t
where t.taskstatus != 'Completed'
group by t.drawer, t.flowname, t.stepname, t.taskstatus
    );
  
  