
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        taskstatus as value_field,
        count(*) as n_records

    from "dev_warehouse"."main"."stg_tasks"
    group by taskstatus

)

select *
from all_values
where value_field not in (
    'Completed','In Progress','Pending','Cancelled'
)



  
  
      
    ) dbt_internal_test