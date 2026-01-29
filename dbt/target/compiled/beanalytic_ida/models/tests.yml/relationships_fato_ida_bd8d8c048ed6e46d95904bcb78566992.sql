
    
    

with child as (
    select nome_grupo as from_field
    from "ida_datamart"."ida"."fato_ida"
    where nome_grupo is not null
),

parent as (
    select nome_grupo as to_field
    from "ida_datamart"."ida"."dim_grupo_economico"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


