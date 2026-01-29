select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select nome_grupo
from "ida_datamart"."ida"."long_delta"
where nome_grupo is null



      
    ) dbt_internal_test