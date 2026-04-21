
    
    

with child as (
    select taskid as from_field
    from "dev_warehouse"."main"."fact_task_rework"
    where taskid is not null
),

parent as (
    select taskid as to_field
    from "dev_warehouse"."main"."fact_task_completed"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


