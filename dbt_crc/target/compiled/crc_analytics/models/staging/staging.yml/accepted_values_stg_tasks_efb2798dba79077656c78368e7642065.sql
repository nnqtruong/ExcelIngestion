
    
    

with all_values as (

    select
        taskstatus as value_field,
        count(*) as n_records

    from "dev_warehouse"."main"."stg_tasks"
    group by taskstatus

)

select *
from all_values
where value_field not in (
    'Completed','In Progress','Pending','Cancelled'
)


