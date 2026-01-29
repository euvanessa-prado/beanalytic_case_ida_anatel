
  
    

  create  table "ida_datamart"."ida"."dim_tempo__dbt_tmp"
  
  
    as
  
  (
    

WITH base AS (
  SELECT DISTINCT
    ano,
    mes,
    ano_mes
  FROM "ida_datamart"."ida"."staging_ida"
)
SELECT
  ROW_NUMBER() OVER (ORDER BY ano, mes) AS id_tempo,
  ano,
  mes,
  ano_mes,
  CASE WHEN mes BETWEEN 1 AND 3 THEN 1
       WHEN mes BETWEEN 4 AND 6 THEN 2
       WHEN mes BETWEEN 7 AND 9 THEN 3
       ELSE 4 END AS trimestre,
  CASE WHEN mes BETWEEN 1 AND 6 THEN 1 ELSE 2 END AS semestre
FROM base
  );
  