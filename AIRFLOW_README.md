# Pipeline ETL com Apache Airflow

Orquestração do pipeline ETL do Data Mart IDA usando Apache Airflow.

## Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                    Apache Airflow                            │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  DAG: ida_etl_pipeline (Execução Mensal)            │   │
│  │                                                       │   │
│  │  1. extrair_dados_portal                            │   │
│  │     └─> Playwright + Web Scraping                   │   │
│  │                                                       │   │
│  │  2. processar_arquivos_ods                          │   │
│  │     └─> Pandas + ODF                                │   │
│  │                                                       │   │
│  │  3. transformar_metricas_ida                        │   │
│  │     └─> Cálculo de métricas                         │   │
│  │                                                       │   │
│  │  4. carregar_dimensoes                              │   │
│  │     └─> dim_tempo, dim_grupo_economico              │   │
│  │                                                       │   │
│  │  5. carregar_fatos                                  │   │
│  │     └─> fato_ida                                    │   │
│  │                                                       │   │
│  │  6. validar_carga                                   │   │
│  │     └─> Verificação de integridade                  │   │
│  │                                                       │   │
│  │  7. limpar_temporarios                              │   │
│  │     └─> Cleanup                                     │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
                ┌───────────────────────┐
                │  PostgreSQL (IDA)     │
                │  - dim_tempo          │
                │  - dim_grupo_economico│
                │  - dim_servico        │
                │  - fato_ida           │
                └───────────────────────┘
```

## Pré-requisitos

- Docker e Docker Compose
- 4GB RAM mínimo
- Portas disponíveis: 8080 (Airflow), 5432 (PostgreSQL IDA), 5433 (PostgreSQL Airflow)

## Instalação e Execução

### 1. Criar diretórios do Airflow

```bash
mkdir -p airflow/logs airflow/plugins airflow/config
```

### 2. Configurar permissões (Linux/Mac)

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### 3. Iniciar serviços

```bash
docker-compose -f docker-compose-airflow.yml up -d
```

Aguarde 2-3 minutos para inicialização completa.

### 4. Acessar Airflow UI

Abra o navegador em: http://localhost:8080

**Credenciais:**
- Username: `admin`
- Password: `admin`

### 5. Ativar a DAG

1. Na interface do Airflow, localize a DAG `ida_etl_pipeline`
2. Clique no toggle para ativar
3. Clique em "Trigger DAG" para executar manualmente

## Estrutura da DAG

### Tasks

1. **extrair_dados_portal**
   - Extrai arquivos ODS do portal dados.gov.br
   - Usa Playwright para web scraping
   - Salva em `dados_ida/`

2. **processar_arquivos_ods**
   - Lê e normaliza arquivos ODS
   - Extrai tipo de serviço (SCM, SMP, STFC)
   - Salva em Parquet temporário

3. **transformar_metricas_ida**
   - Calcula métricas de IDA
   - Agrega dados por período/operadora/serviço
   - Prepara para carga

4. **carregar_dimensoes**
   - Carrega dim_tempo
   - Carrega dim_grupo_economico

5. **carregar_fatos**
   - Carrega fato_ida
   - Relaciona com dimensões

6. **validar_carga**
   - Verifica contagem de registros
   - Valida integridade

7. **limpar_temporarios**
   - Remove arquivos Parquet temporários

### Dependências

```
extrair_dados_portal
    ↓
processar_arquivos_ods
    ↓
transformar_metricas_ida
    ↓
carregar_dimensoes
    ↓
carregar_fatos
    ↓
validar_carga
    ↓
limpar_temporarios
```

## Configuração

### Schedule

A DAG está configurada para executar mensalmente:

```python
schedule_interval='@monthly'
```

Para alterar, edite `airflow/dags/ida_etl_dag.py`:

- `@daily` - Diariamente
- `@weekly` - Semanalmente
- `0 0 * * *` - Diariamente à meia-noite
- `0 9 * * 1` - Segundas-feiras às 9h

### Retries

Configurado para 1 retry com delay de 5 minutos:

```python
'retries': 1,
'retry_delay': timedelta(minutes=5),
```

## Monitoramento

### Ver logs de uma task

1. Na UI do Airflow, clique na DAG
2. Clique na task desejada
3. Clique em "Log"

### Ver XCom (dados compartilhados entre tasks)

1. Admin > XCom
2. Filtrar por DAG ID: `ida_etl_pipeline`

### Métricas disponíveis no XCom

- `arquivos_baixados`: Número de arquivos ODS baixados
- `registros_processados`: Total de registros processados
- `metricas_geradas`: Métricas calculadas
- `fatos_carregados`: Registros carregados na fato
- `validacao`: Contagens de cada tabela

## Consultas SQL

### Verificar última execução

```sql
-- Conectar ao PostgreSQL IDA
docker exec -it ida_postgres psql -U postgres -d ida_datamart

SET search_path TO ida, public;

-- Ver últimos registros carregados
SELECT 
    MAX(data_carga) as ultima_carga,
    COUNT(*) as total_registros
FROM fato_ida;

-- Ver distribuição por serviço
SELECT 
    ds.nome_servico,
    COUNT(*) as registros
FROM fato_ida f
JOIN dim_servico ds ON f.id_servico = ds.id_servico
GROUP BY ds.nome_servico;
```

## Troubleshooting

### DAG não aparece na UI

```bash
# Verificar logs do scheduler
docker-compose -f docker-compose-airflow.yml logs airflow-scheduler

# Verificar se arquivo DAG tem erros de sintaxe
docker-compose -f docker-compose-airflow.yml exec airflow-webserver python /opt/airflow/dags/ida_etl_dag.py
```

### Task falha

1. Ver logs da task na UI
2. Verificar conexão com PostgreSQL:
   ```bash
   docker-compose -f docker-compose-airflow.yml exec airflow-webserver airflow connections list
   ```

### Reiniciar Airflow

```bash
docker-compose -f docker-compose-airflow.yml restart
```

### Limpar metadados e recomeçar

```bash
docker-compose -f docker-compose-airflow.yml down -v
docker-compose -f docker-compose-airflow.yml up -d
```

## Comandos Úteis

### Ver status dos serviços

```bash
docker-compose -f docker-compose-airflow.yml ps
```

### Ver logs

```bash
# Todos os serviços
docker-compose -f docker-compose-airflow.yml logs -f

# Apenas scheduler
docker-compose -f docker-compose-airflow.yml logs -f airflow-scheduler

# Apenas webserver
docker-compose -f docker-compose-airflow.yml logs -f airflow-webserver
```

### Executar comando Airflow

```bash
docker-compose -f docker-compose-airflow.yml exec airflow-webserver airflow dags list
docker-compose -f docker-compose-airflow.yml exec airflow-webserver airflow tasks list ida_etl_pipeline
```

### Parar serviços

```bash
docker-compose -f docker-compose-airflow.yml down
```

## Recursos

- **Airflow UI**: http://localhost:8080
- **PostgreSQL IDA**: localhost:5432
- **PostgreSQL Airflow**: localhost:5433

## Próximos Passos

1. Configurar alertas por email
2. Adicionar sensores para detectar novos arquivos
3. Implementar data quality checks
4. Adicionar testes automatizados
5. Configurar backup automático

## Referências

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
