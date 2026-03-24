



select
    1
from (select * from "dev_warehouse"."main"."mart_tasks_enriched" where lifecycle_hours is not null) dbt_subquery

where not(lifecycle_hours >= 0)

