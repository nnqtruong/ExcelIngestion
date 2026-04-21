



select
    1
from "dev_warehouse"."main"."fact_task_rework"

where not(total_step_events >= 1)

