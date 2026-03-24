
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cost_center
from "dev_warehouse"."main"."mart_team_capacity"
where cost_center is null



  
  
      
    ) dbt_internal_test