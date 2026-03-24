
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "dev_warehouse"."main"."mart_team_demand"

where not(completed >= 0)


  
  
      
    ) dbt_internal_test