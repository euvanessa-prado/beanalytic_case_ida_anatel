
    
    

select
    ano_mes as unique_field,
    count(*) as n_records

from "ida_datamart"."ida"."dim_tempo"
where ano_mes is not null
group by ano_mes
having count(*) > 1


