-- Consolidação de Dimensões e Fato
SET search_path TO ida, public;

COMMENT ON SCHEMA ida IS 'Data Mart do Indicador de Desempenho no Atendimento (IDA).';
COMMENT ON TABLE staging_ida IS 'Camada staging: dados normalizados pré-consolidação.';
COMMENT ON TABLE dim_tempo IS 'Dimensão temporal mensal (ano, mes, trimestre, semestre).';
COMMENT ON TABLE dim_grupo_economico IS 'Cadastro de grupos econômicos padronizados.';
COMMENT ON TABLE dim_servico IS 'Catálogo de serviços (SMP, STFC, SCM).';
COMMENT ON TABLE fato_ida IS 'Tabela fato consolidada com métricas IDA e auxiliares.';

-- 1. Dimensões
INSERT INTO dim_tempo (ano, mes, ano_mes, trimestre, semestre)
SELECT DISTINCT 
    ano, mes, ano_mes,
    CASE WHEN mes BETWEEN 1 AND 3 THEN 1 WHEN mes BETWEEN 4 AND 6 THEN 2 WHEN mes BETWEEN 7 AND 9 THEN 3 ELSE 4 END,
    CASE WHEN mes BETWEEN 1 AND 6 THEN 1 ELSE 2 END
FROM staging_ida
ON CONFLICT (ano_mes) DO NOTHING;

WITH grupo_norm AS (
    SELECT DISTINCT
        TRIM(UPPER(grupo_economico)) as nome_raw,
        CASE 
            WHEN TRIM(UPPER(grupo_economico)) LIKE 'OI%' THEN 'OI'
            WHEN TRIM(UPPER(grupo_economico)) LIKE 'CLARO%' THEN 'CLARO'
            WHEN TRIM(UPPER(grupo_economico)) LIKE 'VIVO%' OR TRIM(UPPER(grupo_economico)) LIKE 'TELEFÔNICA%' THEN 'VIVO'
            WHEN TRIM(UPPER(grupo_economico)) LIKE 'TIM%' THEN 'TIM'
            WHEN TRIM(UPPER(grupo_economico)) LIKE 'CTBC%' OR TRIM(UPPER(grupo_economico)) LIKE 'ALGAR%' THEN 'ALGAR'
            ELSE TRIM(UPPER(grupo_economico))
        END as nome_final
    FROM staging_ida
)
INSERT INTO dim_grupo_economico (nome_grupo)
SELECT DISTINCT nome_final FROM grupo_norm
ON CONFLICT (nome_grupo) DO NOTHING;

INSERT INTO dim_servico (codigo_servico, nome_servico, descricao)
SELECT 
    s,
    CASE 
        WHEN s = 'SMP' THEN 'Serviço Móvel Pessoal'
        WHEN s = 'STFC' THEN 'Serviço Telefônico Fixo Comutado'
        WHEN s = 'SCM' THEN 'Serviço de Comunicação Multimídia'
        ELSE s
    END,
    CASE 
        WHEN s = 'SMP' THEN 'Telefonia Celular'
        WHEN s = 'STFC' THEN 'Telefonia Fixa'
        WHEN s = 'SCM' THEN 'Banda Larga Fixa'
        ELSE 'Outros'
    END
FROM (SELECT DISTINCT UPPER(servico) as s FROM staging_ida) s
ON CONFLICT (codigo_servico) DO UPDATE SET 
    nome_servico = EXCLUDED.nome_servico,
    descricao = EXCLUDED.descricao;

-- 2. Tabela Fato
TRUNCATE TABLE fato_ida CASCADE;

WITH refined AS (
    SELECT 
        ano_mes,
        UPPER(servico) as svc,
        CASE 
            WHEN TRIM(UPPER(grupo_economico)) LIKE 'OI%' THEN 'OI'
            WHEN TRIM(UPPER(grupo_economico)) LIKE 'CLARO%' THEN 'CLARO'
            WHEN TRIM(UPPER(grupo_economico)) LIKE 'VIVO%' OR TRIM(UPPER(grupo_economico)) LIKE 'TELEFÔNICA%' THEN 'VIVO'
            WHEN TRIM(UPPER(grupo_economico)) LIKE 'TIM%' THEN 'TIM'
            WHEN TRIM(UPPER(grupo_economico)) LIKE 'CTBC%' OR TRIM(UPPER(grupo_economico)) LIKE 'ALGAR%' THEN 'ALGAR'
            ELSE TRIM(UPPER(grupo_economico))
        END as grp,
        MAX(CASE WHEN variavel ILIKE 'Indicador de Desempenho%' OR variavel ILIKE 'Índice de Desempenho%' THEN valor END) as val_ida,
        MAX(CASE WHEN variavel ILIKE 'Taxa de Resolvidas em 5 dias%' THEN valor END) as tx_5d,
        MAX(CASE WHEN variavel ILIKE 'Taxa de Resolvidas no Período' THEN valor END) as tx_tot,
        MAX(CASE WHEN variavel IN ('Total de Solicitações', 'Quantidade de Solicitações', 'Quantidade de reclamações', 'Quantidade de Reclamações', 'Quantidade de Reclamações no Período', 'Total de Reclamações') THEN valor END) as sol_tot,
        MAX(CASE WHEN variavel IN ('Quantidade de resolvidas', 'Quantidade de Sol. Resolvidas no Período', 'Quantidade de Respondidas') THEN valor END) as sol_res
    FROM staging_ida
    GROUP BY 1, 2, 3
),
calc AS (
    SELECT 
        *,
        COALESCE(sol_res, ROUND((sol_tot * tx_tot / 100)))::integer as res_final
    FROM refined
)
INSERT INTO fato_ida (
    id_tempo, id_grupo, id_servico, 
    taxa_solicitacoes_resolvidas_5dias,
    taxa_solicitacoes_resolvidas, 
    total_solicitacoes,
    solicitacoes_resolvidas
)
SELECT 
    dt.id_tempo, dg.id_grupo, ds.id_servico,
    COALESCE(c.val_ida, c.tx_5d, 0),
    COALESCE(c.tx_tot, 0),
    COALESCE(c.sol_tot, 0),
    COALESCE(c.res_final, 0)
FROM calc c
JOIN dim_tempo dt ON c.ano_mes = dt.ano_mes
JOIN dim_grupo_economico dg ON c.grp = dg.nome_grupo
JOIN dim_servico ds ON c.svc = ds.codigo_servico;
