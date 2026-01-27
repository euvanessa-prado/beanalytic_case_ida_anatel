"""
DAG do Airflow para Pipeline ETL do Data Mart IDA.

Este DAG orquestra o processo completo de extração, transformação
e carga de dados do Índice de Desempenho no Atendimento (IDA).

Autor: Vanessa Prado
Data: 2026-01-27
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

# Adicionar path do projeto
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

# Argumentos padrão da DAG
default_args = {
    'owner': 'vanessa',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
dag = DAG(
    'ida_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL para Data Mart IDA - Índice de Desempenho no Atendimento',
    schedule_interval='@monthly',  # Executar mensalmente
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'ida', 'anatel', 'telecomunicacoes'],
)


def extrair_dados_portal(**context):
    """
    Task 1: Extrair dados do portal dados.gov.br.
    
    Utiliza web scraping com Playwright para identificar e baixar
    arquivos ODS disponíveis no portal.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info("Iniciando extração de dados do portal...")
    
    # Importar módulos do projeto
    sys.path.insert(0, '/app')
    from baixar_dinamico import AnatelScraper
    
    # Executar scraper
    scraper = AnatelScraper(output_dir='/app/dados_ida')
    arquivos = scraper.executar()
    
    if not arquivos:
        raise Exception("Nenhum arquivo foi baixado do portal!")
    
    logger.info(f"Extração concluída: {len(arquivos)} arquivos baixados")
    
    # Armazenar resultado no XCom
    context['task_instance'].xcom_push(key='arquivos_baixados', value=len(arquivos))
    
    return len(arquivos)


def processar_arquivos_ods(**context):
    """
    Task 2: Processar arquivos ODS.
    
    Lê arquivos ODS, normaliza dados e extrai tipo de serviço.
    """
    import logging
    import pandas as pd
    logger = logging.getLogger(__name__)
    
    logger.info("Iniciando processamento de arquivos ODS...")
    
    # Importar módulos do projeto
    sys.path.insert(0, '/app')
    from ods_processor import ODSProcessor
    
    # Processar arquivos
    processor = ODSProcessor(data_dir='/app/dados_ida')
    df = processor.processar_todos()
    
    if df is None or len(df) == 0:
        raise Exception("Nenhum dado foi processado dos arquivos ODS!")
    
    logger.info(f"Processamento concluído: {len(df)} registros")
    
    # Salvar temporariamente
    df.to_parquet('/tmp/dados_processados.parquet', index=False)
    
    # Armazenar resultado no XCom
    context['task_instance'].xcom_push(key='registros_processados', value=len(df))
    
    return len(df)


def transformar_metricas_ida(**context):
    """
    Task 3: Transformar dados em métricas de IDA.
    
    Calcula métricas agregadas e prepara dados para carga.
    """
    import logging
    import pandas as pd
    logger = logging.getLogger(__name__)
    
    logger.info("Iniciando transformação de métricas IDA...")
    
    # Importar módulos do projeto
    sys.path.insert(0, '/app')
    from transformer import IDATransformer
    
    # Carregar dados processados
    df_raw = pd.read_parquet('/tmp/dados_processados.parquet')
    
    # Transformar
    transformer = IDATransformer()
    df_metricas = transformer.transformar(df_raw)
    
    if df_metricas is None or len(df_metricas) == 0:
        raise Exception("Nenhuma métrica foi gerada!")
    
    # Preparar para carga
    df_final = transformer.preparar_para_carga(df_metricas)
    
    logger.info(f"Transformação concluída: {len(df_final)} registros")
    
    # Salvar temporariamente
    df_final.to_parquet('/tmp/dados_transformados.parquet', index=False)
    
    # Armazenar resultado no XCom
    context['task_instance'].xcom_push(key='metricas_geradas', value=len(df_final))
    
    return len(df_final)


def carregar_dimensoes(**context):
    """
    Task 4: Carregar dimensões no Data Mart.
    
    Carrega dim_tempo e dim_grupo_economico.
    """
    import logging
    import pandas as pd
    logger = logging.getLogger(__name__)
    
    logger.info("Iniciando carga de dimensões...")
    
    # Importar módulos do projeto
    sys.path.insert(0, '/app')
    from loader import DataLoader
    from config import DB_CONFIG
    
    # Carregar dados
    df = pd.read_parquet('/tmp/dados_transformados.parquet')
    
    # Conectar e carregar
    loader = DataLoader(DB_CONFIG)
    loader.connect()
    
    try:
        loader.load_tempo(df)
        loader.load_grupos(df)
        logger.info("Dimensões carregadas com sucesso")
    finally:
        loader.close()
    
    return True


def carregar_fatos(**context):
    """
    Task 5: Carregar tabela fato no Data Mart.
    
    Carrega fato_ida com métricas de desempenho.
    """
    import logging
    import pandas as pd
    logger = logging.getLogger(__name__)
    
    logger.info("Iniciando carga de fatos...")
    
    # Importar módulos do projeto
    sys.path.insert(0, '/app')
    from loader import DataLoader
    from config import DB_CONFIG
    
    # Carregar dados
    df = pd.read_parquet('/tmp/dados_transformados.parquet')
    
    # Conectar e carregar
    loader = DataLoader(DB_CONFIG)
    loader.connect()
    
    try:
        registros = loader.load_fato(df)
        logger.info(f"Fatos carregados: {registros} registros")
        
        # Armazenar resultado no XCom
        context['task_instance'].xcom_push(key='fatos_carregados', value=registros)
    finally:
        loader.close()
    
    return registros


def validar_carga(**context):
    """
    Task 6: Validar carga no Data Mart.
    
    Verifica se os dados foram carregados corretamente.
    """
    import logging
    import psycopg2
    logger = logging.getLogger(__name__)
    
    logger.info("Iniciando validação da carga...")
    
    # Importar configuração
    sys.path.insert(0, '/app')
    from config import DB_CONFIG
    
    # Conectar ao banco
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO ida, public")
    
    try:
        # Contar registros nas tabelas
        cursor.execute("SELECT COUNT(*) FROM dim_tempo")
        count_tempo = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dim_grupo_economico")
        count_grupos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM fato_ida")
        count_fatos = cursor.fetchone()[0]
        
        logger.info(f"Validação - dim_tempo: {count_tempo} registros")
        logger.info(f"Validação - dim_grupo_economico: {count_grupos} registros")
        logger.info(f"Validação - fato_ida: {count_fatos} registros")
        
        # Verificar se há dados
        if count_fatos == 0:
            raise Exception("Tabela fato_ida está vazia!")
        
        # Armazenar resultados no XCom
        context['task_instance'].xcom_push(key='validacao', value={
            'dim_tempo': count_tempo,
            'dim_grupo_economico': count_grupos,
            'fato_ida': count_fatos
        })
        
        logger.info("Validação concluída com sucesso!")
        
    finally:
        cursor.close()
        conn.close()
    
    return True


def limpar_arquivos_temporarios(**context):
    """
    Task 7: Limpar arquivos temporários.
    
    Remove arquivos Parquet temporários.
    """
    import os
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info("Limpando arquivos temporários...")
    
    arquivos = [
        '/tmp/dados_processados.parquet',
        '/tmp/dados_transformados.parquet'
    ]
    
    for arquivo in arquivos:
        if os.path.exists(arquivo):
            os.remove(arquivo)
            logger.info(f"Removido: {arquivo}")
    
    logger.info("Limpeza concluída")
    
    return True


# Definir tasks
task_extrair = PythonOperator(
    task_id='extrair_dados_portal',
    python_callable=extrair_dados_portal,
    provide_context=True,
    dag=dag,
)

task_processar = PythonOperator(
    task_id='processar_arquivos_ods',
    python_callable=processar_arquivos_ods,
    provide_context=True,
    dag=dag,
)

task_transformar = PythonOperator(
    task_id='transformar_metricas_ida',
    python_callable=transformar_metricas_ida,
    provide_context=True,
    dag=dag,
)

task_carregar_dim = PythonOperator(
    task_id='carregar_dimensoes',
    python_callable=carregar_dimensoes,
    provide_context=True,
    dag=dag,
)

task_carregar_fato = PythonOperator(
    task_id='carregar_fatos',
    python_callable=carregar_fatos,
    provide_context=True,
    dag=dag,
)

task_validar = PythonOperator(
    task_id='validar_carga',
    python_callable=validar_carga,
    provide_context=True,
    dag=dag,
)

task_limpar = PythonOperator(
    task_id='limpar_temporarios',
    python_callable=limpar_arquivos_temporarios,
    provide_context=True,
    dag=dag,
)

# Definir dependências (ordem de execução)
task_extrair >> task_processar >> task_transformar >> task_carregar_dim >> task_carregar_fato >> task_validar >> task_limpar
