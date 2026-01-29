select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        codigo_servico as value_field,
        count(*) as n_records

    from "ida_datamart"."ida"."dim_servico"
    group by codigo_servico

)

select *
from all_values
where value_field not in (
    'SMP','STFC','SCM'
)



      
    ) dbt_internal_test