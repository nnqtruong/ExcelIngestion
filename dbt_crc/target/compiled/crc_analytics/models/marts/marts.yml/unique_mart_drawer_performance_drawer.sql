
    
    

select
    drawer as unique_field,
    count(*) as n_records

from "dev_warehouse"."main"."mart_drawer_performance"
where drawer is not null
group by drawer
having count(*) > 1


