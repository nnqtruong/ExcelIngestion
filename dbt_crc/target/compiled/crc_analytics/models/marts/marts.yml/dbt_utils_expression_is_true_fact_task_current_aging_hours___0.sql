



select
    1
from "dev_warehouse"."main"."fact_task_current"

where not(aging_hours >= 0)

