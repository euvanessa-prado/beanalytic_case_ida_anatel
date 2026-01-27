# beAnalytic Case - IDA (Índice de Desempenho no Atendimento)

Pipeline ETL para extração, processamento e análise de dados do Índice de Desempenho no Atendimento (IDA) de operadoras de telecomunicações do portal ANATEL.

## Descrição

Este projeto implementa um pipeline completo de ETL (Extract, Transform, Load) para:
- Extrair arquivos ODS dinamicamente do portal dados.gov.br
- Processar e normalizar dados de desempenho de operadoras
- Carregar dados em Data Mart PostgreSQL
- Gerar análises e visualizações

## Arquitetura

```
projeto_beAnalytic_ida/
├── dados_ida/                      # Arquivos ODS baixados
├── src/                            # Pipeline ETL
│   ├── config.py                   # Configurações
│   ├── loader.py                   # Carregador PostgreSQL
│   ├── main.py                     # Pipeline principal
│   └── ods_processor.py            # Processador de ODS
├── sql/                            # Scripts SQL do Data Mart
│   ├── 01_create_schema.sql
│   ├── 02_create_dimensions.sql
│   ├── 03_create_fato.sql
│   └── 04_create_views.sql
├── baixar_dinamico.py              # Extrator dinâmico (web scraping)
├── processar_ods.py                # Processador standalone
├── docker-compose.yml              # PostgreSQL setup
└── requirements.txt                # Dependências Python
```

## 🚀 Tecnologias

- **Python 3.12+**
- **Playwright** - Web scraping
- **Pandas** - Processamento de dados
- **PostgreSQL** - Data Mart
- **Docker** - Containerização

## 📦 Instalação

```bash
# Clonar repositório
git clone https://github.com/euvanessa-prado/beanalytic_case_ida.git
cd beanalytic_case_ida

# Instalar dependências
pip install -r requirements.txt

# Instalar Playwright
playwright install chromium
```

## 🎯 Uso

### 1. Extrair dados do portal

```bash
python baixar_dinamico.py
```

Extrai dinamicamente arquivos ODS do portal dados.gov.br (19 arquivos: SCM, SMP, STFC de 2013-2019).

### 2. Processar dados

```bash
python processar_ods.py
```

Processa e normaliza 14.749 registros de 29 operadoras.

### 3. Carregar no PostgreSQL

```bash
# Iniciar PostgreSQL
docker-compose up -d

# Executar pipeline ETL
python src/main.py
```

## 📊 Dados

- **Período**: 2013-2019
- **Registros**: 14.749
- **Operadoras**: 29 (OI, TIM, VIVO, CLARO, etc.)
- **Serviços**: SCM (Banda Larga), SMP (Móvel), STFC (Fixo)

## 🏛️ Data Mart

### Dimensões
- `dim_tempo` - Períodos temporais
- `dim_grupo_economico` - Operadoras
- `dim_servico` - Tipos de serviço
- `dim_regiao` - Regiões/UF

### Fato
- `fato_ida` - Métricas de desempenho

### Views
- `vw_taxa_variacao_mensal` - Análise de variação mensal

## 🎨 Padrões de Código

- ✅ Orientação a Objetos (OOP)
- ✅ Documentação pydoc completa
- ✅ Type hints (Python 3.6+)
- ✅ Separação de responsabilidades
- ✅ Clean Code

## 📝 Classes Principais

### baixar_dinamico.py
- `RecursoPortal` - Representa recurso do portal
- `PortalExtractor` - Extrator web scraping
- `ODSDownloader` - Gerenciador de downloads
- `AnatelScraper` - Orquestrador principal

### src/ods_processor.py
- `DataNormalizer` - Normalizador de dados
- `ODSProcessor` - Processador de arquivos ODS

### src/loader.py
- `DatabaseConnection` - Gerenciador de conexão
- `DimensionLoader` - Carregador de dimensões
- `FactLoader` - Carregador de fatos
- `DataLoader` - Orquestrador de carga

## 🔧 Configuração

Variáveis de ambiente (opcional):

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ida_datamart
DB_USER=postgres
DB_PASSWORD=postgres
DATA_DIR=dados_ida
```

## 📈 Resultados

- 19 arquivos ODS extraídos automaticamente
- 14.749 registros processados e normalizados
- Data Mart dimensional implementado
- Pipeline ETL completo e funcional

## 👥 Autor

Vanessa Prado

## 📄 Licença

Este projeto foi desenvolvido como case técnico para beAnalytic.
