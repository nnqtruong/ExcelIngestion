



select
    1
from (select * from "dev_warehouse"."main"."mart_turnaround" where avg_lifecycle_hours is not null) dbt_subquery

where not(avg_lifecycle_hours >= 0)

