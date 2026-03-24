
    
    

select
    row_id as unique_field,
    count(*) as n_records

from "dev_warehouse"."main"."stg_workers"
where row_id is not null
group by row_id
having count(*) > 1


