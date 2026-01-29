select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    nome_grupo as unique_field,
    count(*) as n_records

from "ida_datamart"."ida"."dim_grupo_economico"
where nome_grupo is not null
group by nome_grupo
having count(*) > 1



      
    ) dbt_internal_test