-- ============================================================================
-- Camada Analítica · View de Variação Mensal (Pivotada por Operadora)
-- Objetivo: comparar a variação percentual do **mercado** com a variação
-- percentual **individual** de cada operadora e expor o delta entre elas.
--
-- Regras do cálculo (por mês):
-- 1) Variação do Mercado (%):
--    (IDA_mercado_mês − IDA_mercado_mês_anterior) / IDA_mercado_mês_anterior * 100
-- 2) Variação da Operadora (%):
--    (IDA_op_mês − IDA_op_mês_anterior) / IDA_op_mês_anterior * 100
-- 3) Diferença na view (colunas por operadora):
--    var_mercado − var_individual
--
-- Observação:
-- No dashboard, a leitura é feita como (Individual − Mercado) para que
-- valores positivos indiquem desempenho **acima da média** e negativos **abaixo**.
-- ============================================================================

SET search_path TO ida, public;

DO $$
DECLARE
    v_sql TEXT;
    v_columns TEXT;
BEGIN
    -- Geração dinâmica de colunas para cada Grupo Econômico detectado
    SELECT STRING_AGG(
        FORMAT(
            '    COALESCE(ROUND(MAX(CASE WHEN grupo_base = %L THEN diferenca END)::numeric, 1), 0) as %I',
            grupo, grupo
        ), ',' || CHR(10)
    )
    INTO v_columns
    FROM (
        SELECT DISTINCT TRIM(UPPER(nome_grupo)) as grupo 
        FROM dim_grupo_economico 
        WHERE nome_grupo IS NOT NULL 
        ORDER BY 1
    ) sub;

    v_sql := 'DROP VIEW IF EXISTS vw_taxa_variacao_pivotada CASCADE; ' ||
             'CREATE VIEW vw_taxa_variacao_pivotada AS
    WITH Metricas_Grupadas AS (
        SELECT 
            dt.id_tempo, dt.ano_mes,
            TRIM(UPPER(dg.nome_grupo)) as grupo_base,
            AVG(f.taxa_solicitacoes_resolvidas_5dias) as valor_ida
        FROM fato_ida f
        JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
        JOIN dim_grupo_economico dg ON f.id_grupo = dg.id_grupo
        GROUP BY dt.id_tempo, dt.ano_mes, dg.nome_grupo
    ),
    Media_Mercado AS (
        SELECT id_tempo, ano_mes, AVG(valor_ida) as valor_mercado 
        FROM Metricas_Grupadas
        GROUP BY id_tempo, ano_mes
    ),
    Deltas AS (
        SELECT 
            m.ano_mes, g.grupo_base,
            -- Cálculo de Variação (Market Benchmark)
            CASE 
                WHEN LAG(m.valor_mercado) OVER (PARTITION BY g.grupo_base ORDER BY m.ano_mes) > 0
                THEN ((m.valor_mercado - LAG(m.valor_mercado) OVER (PARTITION BY g.grupo_base ORDER BY m.ano_mes)) 
                      / LAG(m.valor_mercado) OVER (PARTITION BY g.grupo_base ORDER BY m.ano_mes)) * 100
            END as var_mercado,
            -- Cálculo de Variação (Individual)
            CASE 
                WHEN LAG(g.valor_ida) OVER (PARTITION BY g.grupo_base ORDER BY m.ano_mes) > 0
                THEN ((g.valor_ida - LAG(g.valor_ida) OVER (PARTITION BY g.grupo_base ORDER BY m.ano_mes)) 
                      / LAG(g.valor_ida) OVER (PARTITION BY g.grupo_base ORDER BY m.ano_mes)) * 100
            END as var_individual
        FROM Media_Mercado m
        JOIN Metricas_Grupadas g ON m.id_tempo = g.id_tempo
    )
    SELECT 
        ano_mes as \"Mes\",
        COALESCE(ROUND(AVG(var_mercado)::numeric, 1), 0) as \"Taxa de Variação Média\",' || CHR(10) ||
        v_columns || CHR(10) ||
    'FROM (
        SELECT ano_mes, var_mercado, grupo_base, (var_mercado - var_individual) as diferenca 
        FROM Deltas 
        WHERE var_mercado IS NOT NULL
    ) final
    GROUP BY ano_mes ORDER BY \"Mes\";';

    EXECUTE v_sql;
END $$;

COMMENT ON VIEW vw_taxa_variacao_pivotada IS 'Visão pivotada comparando delta de variação entre mercado e operadoras.';
