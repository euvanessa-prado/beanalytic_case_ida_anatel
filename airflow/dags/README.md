# DAG: ida_etl_pipeline

Pipeline ETL para Data Mart IDA - Índice de Desempenho no Atendimento

## Visão Geral

Esta DAG orquestra o processo completo de extração, transformação e carga de dados do portal ANATEL para o Data Mart PostgreSQL.

## Fluxo de Execução

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                   │
│  START                                                            │
│    │                                                              │
│    ▼                                                              │
│  ┌──────────────────────────────────────┐                       │
│  │  1. extrair_dados_portal             │                       │
│  │  - Web scraping com Playwright       │                       │
│  │  - Download de 19 arquivos ODS       │                       │
│  │  - Serviços: SCM, SMP, STFC          │                       │
│  └──────────────────────────────────────┘                       │
│    │                                                              │
│    ▼                                                              │
│  ┌──────────────────────────────────────┐                       │
│  │  2. processar_arquivos_ods           │                       │
│  │  - Leitura de arquivos ODS           │                       │
│  │  - Normalização de dados             │                       │
│  │  - Extração de metadados             │                       │
│  └──────────────────────────────────────┘                       │
│    │                                                              │
│    ▼                                                              │
│  ┌──────────────────────────────────────┐                       │
│  │  3. transformar_metricas_ida         │                       │
│  │  - Cálculo de métricas IDA           │                       │
│  │  - Agregação por período/operadora   │                       │
│  │  - Preparação para carga             │                       │
│  └──────────────────────────────────────┘                       │
│    │                                                              │
│    ▼                                                              │
│  ┌──────────────────────────────────────┐                       │
│  │  4. carregar_dimensoes               │                       │
│  │  - dim_tempo                         │                       │
│  │  - dim_grupo_economico               │                       │
│  └──────────────────────────────────────┘                       │
│    │                                                              │
│    ▼                                                              │
│  ┌──────────────────────────────────────┐                       │
│  │  5. carregar_fatos                   │                       │
│  │  - fato_ida                          │                       │
│  │  - Relacionamento com dimensões      │                       │
│  └──────────────────────────────────────┘                       │
│    │                                                              │
│    ▼                                                              │
│  ┌──────────────────────────────────────┐                       │
│  │  6. validar_carga                    │                       │
│  │  - Verificação de integridade        │                       │
│  │  - Contagem de registros             │                       │
│  └──────────────────────────────────────┘                       │
│    │                                                              │
│    ▼                                                              │
│  ┌──────────────────────────────────────┐                       │
│  │  7. limpar_temporarios               │                       │
│  │  - Remoção de arquivos Parquet       │                       │
│  │  - Cleanup                           │                       │
│  └──────────────────────────────────────┘                       │
│    │                                                              │
│    ▼                                                              │
│  END                                                              │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Configuração

- **DAG ID**: `ida_etl_pipeline`
- **Schedule**: `@monthly` (mensal)
- **Start Date**: 1 dia atrás
- **Catchup**: False
- **Retries**: 1
- **Retry Delay**: 5 minutos

## Tags

- `etl`
- `ida`
- `anatel`
- `telecomunicacoes`

## Dependências Python

- pandas
- psycopg2-binary
- playwright
- odfpy

## Variáveis de Ambiente

- `DB_HOST`: postgres
- `DB_PORT`: 5432
- `DB_NAME`: ida_datamart
- `DB_USER`: postgres
- `DB_PASSWORD`: postgres

## XCom Keys

Dados compartilhados entre tasks:

- `arquivos_baixados`: int - Número de arquivos ODS baixados
- `registros_processados`: int - Total de registros processados
- `metricas_geradas`: int - Métricas calculadas
- `fatos_carregados`: int - Registros carregados na fato
- `validacao`: dict - Contagens de cada tabela

## Monitoramento

### Métricas de Sucesso

- Todos os arquivos ODS baixados (19 esperados)
- Registros processados > 14.000
- Fatos carregados > 0
- Validação sem erros

### Alertas

A DAG falhará se:
- Nenhum arquivo for baixado
- Nenhum dado for processado
- Nenhuma métrica for gerada
- Tabela fato_ida estiver vazia após carga

## Manutenção

### Atualizar DAG

1. Editar `ida_etl_dag.py`
2. Salvar arquivo
3. Aguardar ~30 segundos (Airflow detecta automaticamente)
4. Verificar na UI se DAG foi atualizada

### Executar Manualmente

1. Acessar UI do Airflow
2. Localizar DAG `ida_etl_pipeline`
3. Clicar em "Trigger DAG"
4. Acompanhar execução em tempo real

### Debug

Para debugar uma task específica:

```bash
docker-compose -f docker-compose-airflow.yml exec airflow-webserver \
  airflow tasks test ida_etl_pipeline <task_id> 2026-01-27
```

Exemplo:
```bash
docker-compose -f docker-compose-airflow.yml exec airflow-webserver \
  airflow tasks test ida_etl_pipeline processar_arquivos_ods 2026-01-27
```
