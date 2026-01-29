select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select diferenca
from "ida_datamart"."ida"."long_delta"
where diferenca is null



      
    ) dbt_internal_test