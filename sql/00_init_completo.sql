-- ============================================================================
-- SCRIPT DE INICIALIZACAO COMPLETA DO DATA MART IDA
-- Este script cria toda a estrutura e carrega os dados automaticamente
-- ============================================================================

-- ============================================================================
-- PASSO 1: CRIAR SCHEMA
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS ida;
COMMENT ON SCHEMA ida IS 'Schema para Data Mart de Indice de Desempenho no Atendimento';
SET search_path TO ida, public;

-- ============================================================================
-- PASSO 2: CRIAR DIMENSOES
-- ============================================================================

-- Dimensao Tempo
CREATE TABLE IF NOT EXISTS dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    ano_mes VARCHAR(7) NOT NULL UNIQUE,
    trimestre INTEGER,
    semestre INTEGER,
    CONSTRAINT chk_mes CHECK (mes BETWEEN 1 AND 12),
    CONSTRAINT chk_trimestre CHECK (trimestre BETWEEN 1 AND 4),
    CONSTRAINT chk_semestre CHECK (semestre BETWEEN 1 AND 2)
);

COMMENT ON TABLE dim_tempo IS 'Dimensao temporal com ano, mes, trimestre e semestre';

-- Dimensao Grupo Economico
CREATE TABLE IF NOT EXISTS dim_grupo_economico (
    id_grupo SERIAL PRIMARY KEY,
    nome_grupo VARCHAR(100) NOT NULL UNIQUE,
    ativo BOOLEAN DEFAULT TRUE
);

COMMENT ON TABLE dim_grupo_economico IS 'Dimensao de grupos economicos (operadoras)';

-- Dimensao Servico
CREATE TABLE IF NOT EXISTS dim_servico (
    id_servico SERIAL PRIMARY KEY,
    codigo_servico VARCHAR(10) NOT NULL UNIQUE,
    nome_servico VARCHAR(100) NOT NULL,
    descricao TEXT
);

COMMENT ON TABLE dim_servico IS 'Dimensao de tipos de servico de telecomunicacoes';

-- Inserir servicos padrao
INSERT INTO dim_servico (codigo_servico, nome_servico, descricao) VALUES
('SMP', 'Servico Movel Pessoal', 'Telefonia Celular'),
('STFC', 'Servico Telefonico Fixo Comutado', 'Telefonia Fixa Local'),
('SCM', 'Servico de Comunicacao Multimidia', 'Banda Larga Fixa')
ON CONFLICT (codigo_servico) DO NOTHING;

-- Criar indices
CREATE INDEX IF NOT EXISTS idx_tempo_ano_mes ON dim_tempo(ano, mes);
CREATE INDEX IF NOT EXISTS idx_grupo_nome ON dim_grupo_economico(nome_grupo);
CREATE INDEX IF NOT EXISTS idx_servico_codigo ON dim_servico(codigo_servico);

-- ============================================================================
-- PASSO 3: CRIAR TABELA FATO
-- ============================================================================

CREATE TABLE IF NOT EXISTS fato_ida (
    id_fato SERIAL PRIMARY KEY,
    id_tempo INTEGER NOT NULL REFERENCES dim_tempo(id_tempo),
    id_grupo INTEGER NOT NULL REFERENCES dim_grupo_economico(id_grupo),
    id_servico INTEGER NOT NULL REFERENCES dim_servico(id_servico),
    taxa_resolvidas_5dias DECIMAL(5,2),
    total_solicitacoes INTEGER,
    solicitacoes_resolvidas INTEGER,
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_taxa CHECK (taxa_resolvidas_5dias BETWEEN 0 AND 100)
);

COMMENT ON TABLE fato_ida IS 'Tabela fato com metricas de IDA';

-- Criar indices para performance
CREATE INDEX IF NOT EXISTS idx_fato_tempo ON fato_ida(id_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_grupo ON fato_ida(id_grupo);
CREATE INDEX IF NOT EXISTS idx_fato_servico ON fato_ida(id_servico);
CREATE INDEX IF NOT EXISTS idx_fato_composto ON fato_ida(id_tempo, id_grupo, id_servico);

-- ============================================================================
-- PASSO 4: CRIAR VIEWS
-- ============================================================================

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

-- ============================================================================
-- PASSO 5: CRIAR TABELA STAGING
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging_ida (
    id_staging SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    ano_mes VARCHAR(7) NOT NULL,
    servico VARCHAR(10) NOT NULL,
    grupo_economico VARCHAR(100) NOT NULL,
    variavel VARCHAR(200) NOT NULL,
    valor NUMERIC,
    arquivo_origem VARCHAR(100),
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE staging_ida IS 'Tabela staging para dados brutos dos arquivos ODS';

-- Criar indices para performance
CREATE INDEX IF NOT EXISTS idx_staging_ano_mes ON staging_ida(ano_mes);
CREATE INDEX IF NOT EXISTS idx_staging_servico ON staging_ida(servico);
CREATE INDEX IF NOT EXISTS idx_staging_grupo ON staging_ida(grupo_economico);
CREATE INDEX IF NOT EXISTS idx_staging_variavel ON staging_ida(variavel);

-- ============================================================================
-- MENSAGEM FINAL
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'DATA MART IDA - ESTRUTURA CRIADA COM SUCESSO!';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Schema: ida';
    RAISE NOTICE 'Dimensoes: dim_tempo, dim_grupo_economico, dim_servico';
    RAISE NOTICE 'Fato: fato_ida';
    RAISE NOTICE 'Staging: staging_ida';
    RAISE NOTICE 'Views: vw_taxa_variacao_mensal';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Proximo passo: Execute python carregar_dados.py';
    RAISE NOTICE '============================================================';
END $$;
