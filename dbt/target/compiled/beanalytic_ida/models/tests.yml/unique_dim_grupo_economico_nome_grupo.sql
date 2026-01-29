
    
    

select
    nome_grupo as unique_field,
    count(*) as n_records

from "ida_datamart"."ida"."dim_grupo_economico"
where nome_grupo is not null
group by nome_grupo
having count(*) > 1


