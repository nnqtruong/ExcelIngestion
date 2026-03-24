



select
    1
from "dev_warehouse"."main"."mart_onshore_offshore"

where not(completion_rate >= 0)

