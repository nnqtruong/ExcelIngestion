



select
    1
from (select * from "dev_warehouse"."main"."mart_tasks_enriched" where duration_hours is not null) dbt_subquery

where not(duration_hours >= 0)

