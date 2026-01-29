select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select nome_grupo
from "ida_datamart"."ida"."fato_ida"
where nome_grupo is null



      
    ) dbt_internal_test