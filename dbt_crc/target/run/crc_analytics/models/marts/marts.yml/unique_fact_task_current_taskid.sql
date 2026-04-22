
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    taskid as unique_field,
    count(*) as n_records

from "dev_warehouse"."main"."fact_task_current"
where taskid is not null
group by taskid
having count(*) > 1



  
  
      
    ) dbt_internal_test