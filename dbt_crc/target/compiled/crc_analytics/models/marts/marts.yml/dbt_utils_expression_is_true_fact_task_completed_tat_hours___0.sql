



select
    1
from "dev_warehouse"."main"."fact_task_completed"

where not(tat_hours >= 0)

