



select
    1
from "dev_warehouse"."main"."mart_onshore_offshore"

where not(task_count >= 0)

