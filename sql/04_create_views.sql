-- Criação das views
-- View de Taxa de Variação Mensal

SET search_path TO ida, public;

-- View: Taxa de Variação Mensal
CREATE OR REPLACE VIEW vw_taxa_variacao_mensal AS
WITH taxa_mensal AS (
    SELECT 
        dt.ano_mes,
        dg.nome_grupo,
        AVG(f.taxa_resolvidas_5dias) as taxa_media_grupo
    FROM fato_ida f
    JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
    JOIN dim_grupo_economico dg ON f.id_grupo = dg.id_grupo
    GROUP BY dt.ano_mes, dg.nome_grupo
),
taxa_variacao AS (
    SELECT 
        ano_mes,
        nome_grupo,
        taxa_media_grupo,
        LAG(taxa_media_grupo) OVER (PARTITION BY nome_grupo ORDER BY ano_mes) as taxa_anterior,
        CASE 
            WHEN LAG(taxa_media_grupo) OVER (PARTITION BY nome_grupo ORDER BY ano_mes) IS NOT NULL
            THEN ((taxa_media_grupo - LAG(taxa_media_grupo) OVER (PARTITION BY nome_grupo ORDER BY ano_mes)) 
                  / LAG(taxa_media_grupo) OVER (PARTITION BY nome_grupo ORDER BY ano_mes)) * 100
            ELSE NULL
        END as variacao_percentual
    FROM taxa_mensal
),
taxa_media_geral AS (
    SELECT 
        ano_mes,
        AVG(variacao_percentual) as taxa_variacao_media
    FROM taxa_variacao
    WHERE variacao_percentual IS NOT NULL
    GROUP BY ano_mes
)
SELECT 
    tv.ano_mes as mes,
    ROUND(tmg.taxa_variacao_media::numeric, 2) as taxa_variacao_media,
    tv.nome_grupo,
    ROUND(tv.variacao_percentual::numeric, 2) as variacao_grupo,
    ROUND((tv.variacao_percentual - tmg.taxa_variacao_media)::numeric, 2) as diferenca_da_media
FROM taxa_variacao tv
JOIN taxa_media_geral tmg ON tv.ano_mes = tmg.ano_mes
WHERE tv.variacao_percentual IS NOT NULL
ORDER BY tv.ano_mes, tv.nome_grupo;

COMMENT ON VIEW vw_taxa_variacao_mensal IS 'View com taxa de variação mensal e diferença da média por grupo econômico';
