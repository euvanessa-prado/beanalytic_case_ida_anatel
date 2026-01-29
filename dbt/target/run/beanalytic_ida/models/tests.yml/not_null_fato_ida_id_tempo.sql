select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id_tempo
from "ida_datamart"."ida"."fato_ida"
where id_tempo is null



      
    ) dbt_internal_test