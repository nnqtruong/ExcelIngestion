
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select employee_id
from "dev_warehouse"."main"."stg_workers"
where employee_id is null



  
  
      
    ) dbt_internal_test