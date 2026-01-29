select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    ano_mes as unique_field,
    count(*) as n_records

from "ida_datamart"."ida"."dim_tempo"
where ano_mes is not null
group by ano_mes
having count(*) > 1



      
    ) dbt_internal_test