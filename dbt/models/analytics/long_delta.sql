{{ config(materialized='table', schema='ida') }}

WITH base AS (
  SELECT
    t.ano_mes,
    f.id_tempo,
    f.nome_grupo,
    f.codigo_servico,
    f.taxa_solicitacoes_resolvidas_5dias AS ida
  FROM {{ ref('fato_ida') }} f
  JOIN {{ ref('dim_tempo') }} t ON f.id_tempo = t.id_tempo
),
mercado AS (
  SELECT
    ano_mes,
    AVG(ida) AS valor_mercado
  FROM base
  GROUP BY ano_mes
),
variacoes AS (
  SELECT
    b.ano_mes,
    b.nome_grupo,
    -- Variação do mercado (%): (atual - anterior) / anterior * 100
    CASE
      WHEN LAG(m.valor_mercado) OVER (ORDER BY b.ano_mes) > 0
      THEN ((m.valor_mercado - LAG(m.valor_mercado) OVER (ORDER BY b.ano_mes))
            / LAG(m.valor_mercado) OVER (ORDER BY b.ano_mes)) * 100
    END AS var_mercado,
    -- Variação individual da operadora (%)
    CASE
      WHEN LAG(b.ida) OVER (PARTITION BY b.nome_grupo ORDER BY b.ano_mes) > 0
      THEN ((b.ida - LAG(b.ida) OVER (PARTITION BY b.nome_grupo ORDER BY b.ano_mes))
            / LAG(b.ida) OVER (PARTITION BY b.nome_grupo ORDER BY b.ano_mes)) * 100
    END AS var_individual
  FROM base b
  JOIN mercado m USING (ano_mes)
)
SELECT
  ano_mes,
  nome_grupo,
  ROUND(var_mercado::numeric, 2) AS var_mercado,
  ROUND(var_individual::numeric, 2) AS var_individual,
  ROUND((var_mercado - var_individual)::numeric, 2) AS diferenca
FROM variacoes
WHERE var_mercado IS NOT NULL
  AND var_individual IS NOT NULL
