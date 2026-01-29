
  create view "ida_datamart"."ida"."pivot_variacao__dbt_tmp"
    
    
  as (
    



  


select
  ano_mes as "Mes",
  coalesce(round(avg(var_mercado)::numeric, 1), 0) as "Taxa de Variação Média"
  
  , coalesce(round(max(case when nome_grupo = 'VIVO' then diferenca end)::numeric, 1), 0) as "VIVO"
  
  , coalesce(round(max(case when nome_grupo = 'OI' then diferenca end)::numeric, 1), 0) as "OI"
  
  , coalesce(round(max(case when nome_grupo = 'NET' then diferenca end)::numeric, 1), 0) as "NET"
  
  , coalesce(round(max(case when nome_grupo = 'NEXTEL' then diferenca end)::numeric, 1), 0) as "NEXTEL"
  
  , coalesce(round(max(case when nome_grupo = 'CLARO' then diferenca end)::numeric, 1), 0) as "CLARO"
  
  , coalesce(round(max(case when nome_grupo = 'VIACABO' then diferenca end)::numeric, 1), 0) as "VIACABO"
  
  , coalesce(round(max(case when nome_grupo = 'SERCOMTEL' then diferenca end)::numeric, 1), 0) as "SERCOMTEL"
  
  , coalesce(round(max(case when nome_grupo = 'TIM' then diferenca end)::numeric, 1), 0) as "TIM"
  
  , coalesce(round(max(case when nome_grupo = 'EMBRATEL' then diferenca end)::numeric, 1), 0) as "EMBRATEL"
  
  , coalesce(round(max(case when nome_grupo = 'SKY' then diferenca end)::numeric, 1), 0) as "SKY"
  
  , coalesce(round(max(case when nome_grupo = 'ALGAR' then diferenca end)::numeric, 1), 0) as "ALGAR"
  
  , coalesce(round(max(case when nome_grupo = 'GVT' then diferenca end)::numeric, 1), 0) as "GVT"
  
  , coalesce(round(max(case when nome_grupo = 'INTELIG' then diferenca end)::numeric, 1), 0) as "INTELIG"
  
from "ida_datamart"."ida"."long_delta"
group by 1
order by "Mes"
  );