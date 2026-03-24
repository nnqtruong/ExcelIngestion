



select
    1
from (select * from "dev_warehouse"."main"."mart_team_capacity" where total_fte is not null) dbt_subquery

where not(total_fte >= 0)

