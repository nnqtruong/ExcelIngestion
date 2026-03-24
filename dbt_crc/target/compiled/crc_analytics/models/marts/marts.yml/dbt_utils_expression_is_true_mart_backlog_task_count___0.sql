



select
    1
from "dev_warehouse"."main"."mart_backlog"

where not(task_count >= 0)

