



select
    1
from "dev_warehouse"."main"."mart_turnaround"

where not(completed_count >= 0)

