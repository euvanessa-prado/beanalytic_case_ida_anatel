select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select codigo_servico
from "ida_datamart"."ida"."dim_servico"
where codigo_servico is null



      
    ) dbt_internal_test