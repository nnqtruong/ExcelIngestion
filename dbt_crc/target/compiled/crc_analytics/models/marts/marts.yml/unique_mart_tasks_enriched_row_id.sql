
    
    

select
    row_id as unique_field,
    count(*) as n_records

from "dev_warehouse"."main"."mart_tasks_enriched"
where row_id is not null
group by row_id
having count(*) > 1


