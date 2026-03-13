
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    drawer as unique_field,
    count(*) as n_records

from "dev_warehouse"."main"."mart_drawer_performance"
where drawer is not null
group by drawer
having count(*) > 1



  
  
      
    ) dbt_internal_test