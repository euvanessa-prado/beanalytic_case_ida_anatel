
    
    

with child as (
    select codigo_servico as from_field
    from "ida_datamart"."ida"."fato_ida"
    where codigo_servico is not null
),

parent as (
    select codigo_servico as to_field
    from "ida_datamart"."ida"."dim_servico"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


