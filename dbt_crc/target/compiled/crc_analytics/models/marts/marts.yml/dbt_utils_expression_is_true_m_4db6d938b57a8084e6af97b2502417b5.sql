



select
    1
from (select * from "dev_warehouse"."main"."mart_onshore_offshore" where avg_handle_hours is not null) dbt_subquery

where not(avg_handle_hours >= 0)

