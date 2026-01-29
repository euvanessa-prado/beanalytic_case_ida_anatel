select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select taxa_solicitacoes_resolvidas_5dias
from "ida_datamart"."ida"."fato_ida"
where taxa_solicitacoes_resolvidas_5dias is null



      
    ) dbt_internal_test