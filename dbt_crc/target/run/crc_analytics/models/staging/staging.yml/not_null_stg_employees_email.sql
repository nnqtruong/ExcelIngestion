
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select email
from "dev_warehouse"."main"."stg_employees"
where email is null



  
  
      
    ) dbt_internal_test