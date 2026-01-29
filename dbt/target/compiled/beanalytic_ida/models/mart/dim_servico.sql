

WITH s AS (
  SELECT DISTINCT UPPER(servico) AS codigo_servico
  FROM "ida_datamart"."ida"."staging_ida"
)
SELECT 
  codigo_servico,
  CASE 
    WHEN codigo_servico = 'SMP' THEN 'Serviço Móvel Pessoal'
    WHEN codigo_servico = 'STFC' THEN 'Serviço Telefônico Fixo Comutado'
    WHEN codigo_servico = 'SCM' THEN 'Serviço de Comunicação Multimídia'
    ELSE codigo_servico
  END AS nome_servico,
  CASE 
    WHEN codigo_servico = 'SMP' THEN 'Telefonia Celular'
    WHEN codigo_servico = 'STFC' THEN 'Telefonia Fixa'
    WHEN codigo_servico = 'SCM' THEN 'Banda Larga Fixa'
    ELSE 'Outros'
  END AS descricao
FROM s