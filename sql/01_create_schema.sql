-- Criação do schema do Data Mart IDA
-- Autor: Vanessa
-- Data: 2026-01-26

-- Criar schema se não existir
CREATE SCHEMA IF NOT EXISTS ida;

-- Comentário no schema
COMMENT ON SCHEMA ida IS 'Schema para Data Mart de Índice de Desempenho no Atendimento';

-- Configurações
SET search_path TO ida, public;
