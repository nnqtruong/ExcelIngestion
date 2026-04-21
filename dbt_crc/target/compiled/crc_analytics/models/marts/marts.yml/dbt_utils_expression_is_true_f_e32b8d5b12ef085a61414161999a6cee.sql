



select
    1
from "dev_warehouse"."main"."fact_task_rework"

where not(rework_step_events >= 0)

