
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select name
from "dev_warehouse"."main"."stg_employees_master"
where name is null



  
  
      
    ) dbt_internal_test