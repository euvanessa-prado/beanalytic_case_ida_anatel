-- Script de transformacao e carga dos dados da staging para o Data Mart
-- Este script processa os dados brutos e popula as dimensoes e fato

SET search_path TO ida, public;

-- ============================================================================
-- PASSO 1: POPULAR DIMENSAO TEMPO
-- ============================================================================
INSERT INTO dim_tempo (ano, mes, ano_mes, trimestre, semestre)
SELECT DISTINCT
    ano,
    mes,
    ano_mes,
    CASE 
        WHEN mes BETWEEN 1 AND 3 THEN 1
        WHEN mes BETWEEN 4 AND 6 THEN 2
        WHEN mes BETWEEN 7 AND 9 THEN 3
        ELSE 4
    END as trimestre,
    CASE 
        WHEN mes BETWEEN 1 AND 6 THEN 1
        ELSE 2
    END as semestre
FROM staging_ida
ON CONFLICT (ano_mes) DO NOTHING;

-- ============================================================================
-- PASSO 2: POPULAR DIMENSAO GRUPO ECONOMICO
-- ============================================================================
INSERT INTO dim_grupo_economico (nome_grupo)
SELECT DISTINCT grupo_economico
FROM staging_ida
WHERE grupo_economico IS NOT NULL
ON CONFLICT (nome_grupo) DO NOTHING;

-- ============================================================================
-- PASSO 3: TRANSFORMAR E CARREGAR FATO IDA
-- ============================================================================

-- Criar tabela temporaria com dados pivotados
CREATE TEMP TABLE temp_ida_pivot AS
SELECT 
    ano,
    mes,
    ano_mes,
    servico,
    grupo_economico,
    MAX(CASE WHEN variavel = 'Indicador de Desempenho no Atendimento (IDA)' THEN valor END) as ida_percentual,
    MAX(CASE WHEN variavel = 'Taxa de Resolvidas em 5 dias Úteis' THEN valor END) as taxa_resolvidas_5dias,
    MAX(CASE WHEN variavel = 'Taxa de Respondidas em 5 dias Úteis' THEN valor END) as taxa_respondidas_5dias,
    MAX(CASE WHEN variavel = 'Quantidade de Respondidas' THEN valor END) as qtd_respondidas,
    MAX(CASE WHEN variavel = 'Quantidade de Sol. Respondidas no Período' THEN valor END) as qtd_sol_respondidas_periodo,
    MAX(CASE WHEN variavel = 'Quantidade de Sol. Respondidas em até 5 dias' THEN valor END) as qtd_sol_respondidas_5dias,
    MAX(CASE WHEN variavel = 'Quantidade de Sol. Resolvidas no Período' THEN valor END) as qtd_sol_resolvidas_periodo,
    MAX(CASE WHEN variavel = 'Quantidade de Sol. Resolvidas em até 5 dias' THEN valor END) as qtd_sol_resolvidas_5dias
FROM staging_ida
WHERE variavel IN (
    'Indicador de Desempenho no Atendimento (IDA)',
    'Taxa de Resolvidas em 5 dias Úteis',
    'Taxa de Respondidas em 5 dias Úteis',
    'Quantidade de Respondidas',
    'Quantidade de Sol. Respondidas no Período',
    'Quantidade de Sol. Respondidas em até 5 dias',
    'Quantidade de Sol. Resolvidas no Período',
    'Quantidade de Sol. Resolvidas em até 5 dias'
)
GROUP BY ano, mes, ano_mes, servico, grupo_economico;

-- Inserir dados na tabela fato com transformacoes
INSERT INTO fato_ida (
    id_tempo,
    id_grupo,
    id_servico,
    taxa_resolvidas_5dias,
    total_solicitacoes,
    solicitacoes_resolvidas
)
SELECT 
    dt.id_tempo,
    dg.id_grupo,
    ds.id_servico,
    -- Taxa resolvidas/respondidas em 5 dias (usar a taxa direta ou calcular)
    CASE 
        WHEN p.taxa_resolvidas_5dias IS NOT NULL THEN
            LEAST(100, GREATEST(0, ROUND(p.taxa_resolvidas_5dias::numeric, 2)))
        WHEN p.taxa_respondidas_5dias IS NOT NULL THEN
            LEAST(100, GREATEST(0, ROUND(p.taxa_respondidas_5dias::numeric, 2)))
        WHEN p.ida_percentual IS NOT NULL THEN
            LEAST(100, GREATEST(0, ROUND(p.ida_percentual::numeric, 2)))
        ELSE 0
    END as taxa_resolvidas_5dias,
    -- Total de solicitacoes (usar Quantidade de Respondidas se disponivel, senao usar outras variaveis)
    COALESCE(
        p.qtd_respondidas,
        p.qtd_sol_respondidas_periodo,
        p.qtd_sol_resolvidas_periodo,
        0
    )::integer as total_solicitacoes,
    -- Solicitacoes resolvidas/respondidas em ate 5 dias
    COALESCE(
        p.qtd_sol_resolvidas_5dias,
        p.qtd_sol_respondidas_5dias,
        0
    )::integer as solicitacoes_resolvidas
FROM temp_ida_pivot p
JOIN dim_tempo dt ON p.ano_mes = dt.ano_mes
JOIN dim_grupo_economico dg ON p.grupo_economico = dg.nome_grupo
JOIN dim_servico ds ON p.servico = ds.codigo_servico;

-- Limpar tabela temporaria
DROP TABLE IF EXISTS temp_ida_pivot;

-- ============================================================================
-- ESTATISTICAS DE CARGA
-- ============================================================================
DO $$
DECLARE
    v_count_staging INTEGER;
    v_count_tempo INTEGER;
    v_count_grupos INTEGER;
    v_count_fato INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count_staging FROM staging_ida;
    SELECT COUNT(*) INTO v_count_tempo FROM dim_tempo;
    SELECT COUNT(*) INTO v_count_grupos FROM dim_grupo_economico;
    SELECT COUNT(*) INTO v_count_fato FROM fato_ida;
    
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'ESTATISTICAS DE CARGA';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Registros na staging: %', v_count_staging;
    RAISE NOTICE 'Registros em dim_tempo: %', v_count_tempo;
    RAISE NOTICE 'Registros em dim_grupo_economico: %', v_count_grupos;
    RAISE NOTICE 'Registros em fato_ida: %', v_count_fato;
    RAISE NOTICE '============================================================';
END $$;
