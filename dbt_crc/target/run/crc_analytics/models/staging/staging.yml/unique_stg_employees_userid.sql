
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    userid as unique_field,
    count(*) as n_records

from "dev_warehouse"."main"."stg_employees"
where userid is not null
group by userid
having count(*) > 1



  
  
      
    ) dbt_internal_test