



select
    1
from "dev_warehouse"."main"."mart_daily_trend"

where not(tasks_completed >= 0)

