



select
    1
from (select * from "dev_warehouse"."main"."fact_task_event" where queue_wait_hours is not null) dbt_subquery

where not(queue_wait_hours >= 0)

