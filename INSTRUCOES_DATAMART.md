# Instruções para Execução do Data Mart IDA

## Pré-requisitos

- Docker e Docker Compose instalados
- Python 3.11+ instalado
- Arquivos ODS já baixados na pasta `dados_ida/`

## Passo a Passo

### 1. Extrair Dados do Portal (se ainda não fez)

```bash
python baixar_dinamico.py
```

Isso irá baixar 19 arquivos ODS do portal dados.gov.br para a pasta `dados_ida/`.

### 2. Iniciar PostgreSQL com Docker

```bash
docker-compose up -d postgres
```

Aguarde alguns segundos para o PostgreSQL inicializar completamente.

### 3. Verificar se PostgreSQL está pronto

```bash
docker-compose ps
```

O status deve mostrar "healthy" para o serviço postgres.

### 4. Executar Pipeline ETL

```bash
# Instalar dependências (se ainda não instalou)
pip install -r requirements.txt

# Executar pipeline
python src/main.py
```

O pipeline irá:
1. Extrair dados dos 19 arquivos ODS
2. Normalizar e transformar em métricas de IDA
3. Carregar no Data Mart PostgreSQL (modelo estrela)

### 5. Acessar o Data Mart

Conecte-se ao PostgreSQL:

```bash
# Via Docker
docker exec -it ida_postgres psql -U postgres -d ida_datamart

# Ou via cliente PostgreSQL
psql -h localhost -p 5432 -U postgres -d ida_datamart
```

Senha: `postgres`

### 6. Consultar Dados

```sql
-- Configurar schema
SET search_path TO ida, public;

-- Ver dimensões
SELECT * FROM dim_tempo LIMIT 10;
SELECT * FROM dim_grupo_economico;
SELECT * FROM dim_servico;

-- Ver fatos
SELECT * FROM fato_ida LIMIT 10;

-- View de análise
SELECT * FROM vw_taxa_variacao_mensal
WHERE nome_grupo IN ('OI', 'TIM', 'VIVO', 'CLARO')
ORDER BY mes, nome_grupo;
```

## Estrutura do Data Mart

### Modelo Estrela

```
                    dim_tempo
                        |
                        |
dim_grupo_economico --- fato_ida --- dim_servico
                        |
                        |
                    dim_regiao
```

### Dimensões

- **dim_tempo**: Períodos temporais (ano, mês, trimestre, semestre)
- **dim_grupo_economico**: Operadoras (OI, TIM, VIVO, CLARO, etc.)
- **dim_servico**: Tipos de serviço (SMP, STFC, SCM)
- **dim_regiao**: Regiões/UF (não utilizada - dados nacionais)

### Tabela Fato

- **fato_ida**: Métricas de desempenho
  - taxa_resolvidas_5dias: % de solicitações resolvidas em 5 dias
  - total_solicitacoes: Total de solicitações
  - solicitacoes_resolvidas: Número de solicitações resolvidas

## Consultas Úteis

### Comparar IDA por Operadora

```sql
SELECT 
    dg.nome_grupo,
    ds.nome_servico,
    AVG(f.taxa_resolvidas_5dias) as taxa_media
FROM fato_ida f
JOIN dim_grupo_economico dg ON f.id_grupo = dg.id_grupo
JOIN dim_servico ds ON f.id_servico = ds.id_servico
GROUP BY dg.nome_grupo, ds.nome_servico
ORDER BY taxa_media DESC;
```

### Evolução Temporal por Serviço

```sql
SELECT 
    dt.ano_mes,
    ds.codigo_servico,
    AVG(f.taxa_resolvidas_5dias) as taxa_media
FROM fato_ida f
JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
JOIN dim_servico ds ON f.id_servico = ds.id_servico
GROUP BY dt.ano_mes, ds.codigo_servico
ORDER BY dt.ano_mes, ds.codigo_servico;
```

### Top 5 Operadoras por Serviço

```sql
SELECT 
    ds.nome_servico,
    dg.nome_grupo,
    AVG(f.taxa_resolvidas_5dias) as taxa_media
FROM fato_ida f
JOIN dim_grupo_economico dg ON f.id_grupo = dg.id_grupo
JOIN dim_servico ds ON f.id_servico = ds.id_servico
GROUP BY ds.nome_servico, dg.nome_grupo
ORDER BY ds.nome_servico, taxa_media DESC;
```

## Parar Serviços

```bash
docker-compose down
```

## Limpar Dados e Recomeçar

```bash
# Parar e remover containers e volumes
docker-compose down -v

# Reiniciar
docker-compose up -d postgres
python src/main.py
```

## Troubleshooting

### Erro de Conexão com PostgreSQL

Aguarde mais tempo para o PostgreSQL inicializar:

```bash
docker-compose logs postgres
```

### Erro "Nenhum arquivo ODS encontrado"

Execute primeiro:

```bash
python baixar_dinamico.py
```

### Erro de Dependências Python

```bash
pip install --upgrade -r requirements.txt
```

## Arquitetura

- **PostgreSQL 17.5**: Banco de dados
- **Python 3.11**: Pipeline ETL
- **Docker Compose**: Orquestração de containers
- **Pandas**: Processamento de dados
- **psycopg2**: Conexão PostgreSQL
