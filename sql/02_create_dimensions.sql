-- Criação das tabelas dimensão
-- Modelo Estrela - Data Mart IDA

SET search_path TO ida, public;

-- Dimensão Tempo
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

COMMENT ON TABLE dim_tempo IS 'Dimensão temporal com ano, mês, trimestre e semestre';
COMMENT ON COLUMN dim_tempo.ano_mes IS 'Formato YYYY-MM para facilitar ordenação';

-- Dimensão Grupo Econômico
CREATE TABLE IF NOT EXISTS dim_grupo_economico (
    id_grupo SERIAL PRIMARY KEY,
    nome_grupo VARCHAR(100) NOT NULL UNIQUE,
    ativo BOOLEAN DEFAULT TRUE
);

COMMENT ON TABLE dim_grupo_economico IS 'Dimensão de grupos econômicos (operadoras)';
COMMENT ON COLUMN dim_grupo_economico.nome_grupo IS 'Nome do grupo econômico (CLARO, VIVO, TIM, OI, etc)';

-- Dimensão Serviço
CREATE TABLE IF NOT EXISTS dim_servico (
    id_servico SERIAL PRIMARY KEY,
    codigo_servico VARCHAR(10) NOT NULL UNIQUE,
    nome_servico VARCHAR(100) NOT NULL,
    descricao TEXT
);

COMMENT ON TABLE dim_servico IS 'Dimensão de tipos de serviço de telecomunicações';
COMMENT ON COLUMN dim_servico.codigo_servico IS 'Código do serviço: SMP, STFC ou SCM';

-- Inserir serviços padrão
INSERT INTO dim_servico (codigo_servico, nome_servico, descricao) VALUES
('SMP', 'Serviço Móvel Pessoal', 'Telefonia Celular'),
('STFC', 'Serviço Telefônico Fixo Comutado', 'Telefonia Fixa Local'),
('SCM', 'Serviço de Comunicação Multimídia', 'Banda Larga Fixa')
ON CONFLICT (codigo_servico) DO NOTHING;

-- Dimensão Região
CREATE TABLE IF NOT EXISTS dim_regiao (
    id_regiao SERIAL PRIMARY KEY,
    uf VARCHAR(2) NOT NULL UNIQUE,
    regiao VARCHAR(20),
    nome_estado VARCHAR(50)
);

COMMENT ON TABLE dim_regiao IS 'Dimensão geográfica com UF e região';
COMMENT ON COLUMN dim_regiao.uf IS 'Sigla da Unidade Federativa';

-- Criar índices
CREATE INDEX IF NOT EXISTS idx_tempo_ano_mes ON dim_tempo(ano, mes);
CREATE INDEX IF NOT EXISTS idx_grupo_nome ON dim_grupo_economico(nome_grupo);
CREATE INDEX IF NOT EXISTS idx_servico_codigo ON dim_servico(codigo_servico);
CREATE INDEX IF NOT EXISTS idx_regiao_uf ON dim_regiao(uf);
