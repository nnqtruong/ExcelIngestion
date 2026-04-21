



select
    1
from "dev_warehouse"."main"."fact_task_rework"

where not(rework_event_rate <= 1)

