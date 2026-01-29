-- ========================================================
-- Schema IDA - Data Mart Core
-- Descrição: Tabelas principais para o Indicador de Desempenho Anatel.
-- ========================================================

CREATE SCHEMA IF NOT EXISTS ida;
SET search_path TO ida, public;

CREATE TABLE IF NOT EXISTS dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    ano_mes VARCHAR(7) NOT NULL UNIQUE,
    trimestre INTEGER,
    semestre INTEGER
);

COMMENT ON TABLE dim_tempo IS 'Dimensão temporal mensal para análise de séries históricas.';
COMMENT ON COLUMN dim_tempo.ano_mes IS 'Chave de competência formatada YYYY-MM.';

CREATE TABLE IF NOT EXISTS dim_grupo_economico (
    id_grupo SERIAL PRIMARY KEY,
    nome_grupo VARCHAR(100) NOT NULL UNIQUE,
    ativo BOOLEAN DEFAULT TRUE
);

COMMENT ON TABLE dim_grupo_economico IS 'Entidades econômicas das operadoras monitoradas.';

CREATE TABLE IF NOT EXISTS dim_servico (
    id_servico SERIAL PRIMARY KEY,
    codigo_servico VARCHAR(10) NOT NULL UNIQUE,
    nome_servico VARCHAR(100) NOT NULL,
    descricao TEXT
);

COMMENT ON TABLE dim_servico IS 'Modalidades de serviço (SMP, STFC, SCM).';

CREATE TABLE IF NOT EXISTS fato_ida (
    id_fato SERIAL PRIMARY KEY,
    id_tempo INTEGER NOT NULL REFERENCES dim_tempo(id_tempo),
    id_grupo INTEGER NOT NULL REFERENCES dim_grupo_economico(id_grupo),
    id_servico INTEGER NOT NULL REFERENCES dim_servico(id_servico),
    taxa_solicitacoes_resolvidas_5dias DECIMAL(5,2),
    taxa_solicitacoes_resolvidas DECIMAL(5,2),
    total_solicitacoes INTEGER,
    solicitacoes_resolvidas INTEGER,
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE fato_ida IS 'Tabela fato contendo métricas de desempenho e resolutividade.';
COMMENT ON COLUMN fato_ida.taxa_solicitacoes_resolvidas_5dias IS 'Taxa de atendimento em até 5 dias úteis.';
COMMENT ON COLUMN fato_ida.taxa_solicitacoes_resolvidas IS 'Taxa total de solicitações atendidas.';

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

COMMENT ON TABLE staging_ida IS 'Camada de staging para ingestão bruta dos arquivos ODS.';

-- Índices
CREATE INDEX IF NOT EXISTS idx_fato_lookup ON fato_ida(id_tempo, id_grupo, id_servico);
CREATE INDEX IF NOT EXISTS idx_stg_comp ON staging_ida(ano_mes, grupo_economico);
