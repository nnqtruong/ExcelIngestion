
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select completed
from "dev_warehouse"."main"."mart_team_demand"
where completed is null



  
  
      
    ) dbt_internal_test