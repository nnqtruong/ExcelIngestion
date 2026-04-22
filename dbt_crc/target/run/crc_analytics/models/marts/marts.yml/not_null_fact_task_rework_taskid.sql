
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select taskid
from "dev_warehouse"."main"."fact_task_rework"
where taskid is null



  
  
      
    ) dbt_internal_test