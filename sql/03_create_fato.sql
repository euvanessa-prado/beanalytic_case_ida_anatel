-- Criação da tabela fato
-- Modelo Estrela - Data Mart IDA

SET search_path TO ida, public;

-- Tabela Fato IDA
CREATE TABLE IF NOT EXISTS fato_ida (
    id_fato SERIAL PRIMARY KEY,
    id_tempo INTEGER NOT NULL REFERENCES dim_tempo(id_tempo),
    id_grupo INTEGER NOT NULL REFERENCES dim_grupo_economico(id_grupo),
    id_servico INTEGER NOT NULL REFERENCES dim_servico(id_servico),
    id_regiao INTEGER REFERENCES dim_regiao(id_regiao),
    taxa_resolvidas_5dias DECIMAL(5,2),
    total_solicitacoes INTEGER,
    solicitacoes_resolvidas INTEGER,
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_taxa CHECK (taxa_resolvidas_5dias BETWEEN 0 AND 100)
);

COMMENT ON TABLE fato_ida IS 'Tabela fato com métricas de IDA';
COMMENT ON COLUMN fato_ida.taxa_resolvidas_5dias IS 'Percentual de solicitações resolvidas em 5 dias úteis';
COMMENT ON COLUMN fato_ida.total_solicitacoes IS 'Total de solicitações recebidas';
COMMENT ON COLUMN fato_ida.solicitacoes_resolvidas IS 'Número de solicitações resolvidas';

-- Criar índices para performance
CREATE INDEX IF NOT EXISTS idx_fato_tempo ON fato_ida(id_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_grupo ON fato_ida(id_grupo);
CREATE INDEX IF NOT EXISTS idx_fato_servico ON fato_ida(id_servico);
CREATE INDEX IF NOT EXISTS idx_fato_regiao ON fato_ida(id_regiao);
CREATE INDEX IF NOT EXISTS idx_fato_composto ON fato_ida(id_tempo, id_grupo, id_servico);
