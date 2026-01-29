select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ano_mes
from "ida_datamart"."ida"."long_delta"
where ano_mes is null



      
    ) dbt_internal_test