

WITH refined AS (
  SELECT 
    ano_mes,
    UPPER(servico) AS svc,
    CASE 
      WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'OI%' THEN 'OI'
      WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'CLARO%' THEN 'CLARO'
      WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'VIVO%' OR REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'TELEFÔNICA%' THEN 'VIVO'
      WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'TIM%' THEN 'TIM'
      WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'CTBC%' OR REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'ALGAR%' THEN 'ALGAR'
      WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'SERCOMTEL%' THEN 'SERCOMTEL'
      ELSE REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g')
    END AS grp,
    MAX(CASE WHEN variavel ILIKE 'Indicador de Desempenho%' OR variavel ILIKE 'Índice de Desempenho%' THEN valor END) AS val_ida,
    MAX(CASE WHEN variavel ILIKE 'Taxa de Resolvidas em 5 dias%' THEN valor END) AS tx_5d,
    MAX(CASE WHEN variavel ILIKE 'Taxa de Resolvidas no Período' THEN valor END) AS tx_tot,
    MAX(CASE WHEN variavel IN ('Total de Solicitações', 'Quantidade de Solicitações', 'Quantidade de reclamações', 'Quantidade de Reclamações', 'Quantidade de Reclamações no Período', 'Total de Reclamações') THEN valor END) AS sol_tot,
    MAX(CASE WHEN variavel IN ('Quantidade de resolvidas', 'Quantidade de Sol. Resolvidas no Período', 'Quantidade de Respondidas') THEN valor END) AS sol_res
  FROM "ida_datamart"."ida"."staging_ida"
  GROUP BY 1, 2, 3
),
calc AS (
  SELECT 
    *,
    COALESCE(sol_res, ROUND((sol_tot * tx_tot / 100)))::integer AS res_final
  FROM refined
)
SELECT 
  dt.id_tempo, dg.nome_grupo, ds.codigo_servico,
  COALESCE(c.val_ida, c.tx_5d, 0) AS taxa_solicitacoes_resolvidas_5dias,
  COALESCE(c.tx_tot, 0) AS taxa_solicitacoes_resolvidas,
  COALESCE(c.sol_tot, 0) AS total_solicitacoes,
  COALESCE(c.res_final, 0) AS solicitacoes_resolvidas
FROM calc c
JOIN "ida_datamart"."ida"."dim_tempo" dt ON c.ano_mes = dt.ano_mes
JOIN "ida_datamart"."ida"."dim_grupo_economico" dg ON c.grp = dg.nome_grupo
JOIN "ida_datamart"."ida"."dim_servico" ds ON c.svc = ds.codigo_servico