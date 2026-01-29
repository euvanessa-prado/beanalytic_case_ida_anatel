# Case Técnico: Data Mart IDA Anatel

## 📌 Visão Geral
Este projeto implementa uma solução de Engenharia de Dados ponta a ponta para a ingestão, tratamento e modelagem analítica dos dados do **Índice de Desempenho no Atendimento (IDA)** da Anatel. A solução automatiza a extração de arquivos OpenDocument (.ods), normaliza estruturas variadas através de processamento Python e consolida as métricas em um Data Mart PostgreSQL seguindo o modelo dimensional (Star Schema).

## 🏗️ Arquitetura da Solução

### 1. Camada de Ingestão (Python)
- **ODS Processor**: Motor em Python que utiliza `pandas` e `odfpy` para ler planilhas brutas.
- **Normalização**: Implementação de lógica robusta para converter tabelas dinâmicas (wide format) em dados normalizados (long format), tratando inconsistências de cabeçalhos e períodos.
- **Bulk Loading**: Persistência otimizada na Staging via `psycopg2.extras.execute_values`.

### 2. Camada de Dados (PostgreSQL)
- **Staging**: Camada temporária para persistência dos dados brutos normalizados.
- **Trusted/Refined (Dimensional)**:
    - `dim_tempo`: Dimensão temporal com atributos de calendário.
    - `dim_grupo_economico`: Cadastro único de operadoras.
    - `dim_servico`: Classificação dos serviços de telecomunicação.
    - `fato_ida`: Tabela fato contendo métricas de agilidade (5 dias), resolução total e volumes de solicitações.

### 3. Camada Analítica (SQL)
- **View Pivotada**: Implementação de SQL dinâmico para gerar relatórios de variação percentual mês a mês, permitindo comparação direta entre o desempenho individual das operadoras e a média do mercado.
- **Dashboard Interativo**: Interface gráfica desenvolvida em **Streamlit** para visualização amigável dos dados, permitindo análises de tendência e heatmaps comparativos.

## 🚀 Como Executar

A solução é totalmente conteinerizada via Docker. Siga os passos abaixo:

1. **Pré-requisitos**:
   - Docker e Docker Compose instalados.

2. **Subir tudo (um comando)**:
   ```bash
   docker compose up -d
   ```
   - Na primeira execução, o Compose constrói as imagens automaticamente.
   - Após remover volumes (reset), os dados são recarregados pelo ETL.

3. **Fluxo Automático**:
   - O banco PostgreSQL é inicializado com o schema base.
   - O container `data_loader` aguarda o banco estar `healthy`.
   - Inicia o processamento dos arquivos presentes em `dados_ida/`.
- Executa as transformações SQL para carga da Fato e criação das Views:
  - [01_transform_load.sql](file:///home/vanessa-aws/projeto_beAnalytic_copia/sql/01_transform_load.sql)
  - [02_view_pivotada.sql](file:///home/vanessa-aws/projeto_beAnalytic_copia/sql/02_view_pivotada.sql)

4. **Ver logs rapidamente**:
   ```bash
   docker compose logs -f data_loader
   ```
   - Aguarde a mensagem: `ETL completed successfully`.

5. **Reset opcional (apagar dados e subir limpo)**:
   ```bash
   docker compose down -v && docker compose up -d
   ```
   - O ETL recria o Data Mart automaticamente.

## 🎯 Objetos de Avaliação (Prova Técnica)
- O projeto roda integralmente com `docker compose up` (infra + ETL + dashboard).
- SQL: organização, clareza e documentação usando `COMMENT ON`.
- Python: organização, clareza, docstrings (pydoc) e uso de OOP.
- Sem dependência de scripts externos; instruções mínimas e diretas.

## 📁 Estrutura do Projeto
- `/sql`: Scripts de DDL, Transformação e Views.
- `/src`: Módulos Python (Normalização e Carregamento).
- `/dados_ida`: Repositório de arquivos brutos (.ods).
- `docker-compose.yml`: Orquestração da infraestrutura.
- `/assets`: Recursos gráficos do dashboard.

## 📊 Principais Métricas
- **Taxa de Variação Individual**: Evolução percentual do IDA de uma operadora.
- **Benchmarking de Mercado**: Comparação da variação individual contra a média do setor.
- **Métrica de Agilidade**: Taxa de resolvidas em até 5 dias úteis.
# beanalytic_case_ida_anatel
