
    
    

select
    taskid as unique_field,
    count(*) as n_records

from "dev_warehouse"."main"."fact_task_rework"
where taskid is not null
group by taskid
having count(*) > 1


