"""
Script simples para carregar dados dos arquivos ODS no PostgreSQL.

Este script:
1. Processa os arquivos ODS da pasta dados_ida/
2. Carrega os dados brutos na tabela staging
3. Executa as transformacoes SQL para popular o Data Mart

Uso:
    python carregar_dados.py
"""

import sys
import time
import logging
import subprocess

# Adicionar src ao path
sys.path.insert(0, 'src')

from ods_processor import ODSProcessor
from staging_loader import StagingLoader
from config import DB_CONFIG, DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def aguardar_postgres():
    """Aguarda o PostgreSQL estar pronto."""
    logger.info("Aguardando PostgreSQL iniciar...")
    max_tentativas = 30
    
    for i in range(max_tentativas):
        try:
            import psycopg2
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            logger.info("PostgreSQL esta pronto!")
            return True
        except Exception:
            if i < max_tentativas - 1:
                time.sleep(2)
            else:
                logger.error("PostgreSQL nao iniciou a tempo")
                return False
    
    return False


def processar_ods():
    """Processa arquivos ODS e retorna DataFrame."""
    logger.info("="*80)
    logger.info("PASSO 1: PROCESSAR ARQUIVOS ODS")
    logger.info("="*80)
    
    processor = ODSProcessor(DATA_DIR)
    df = processor.processar_todos()
    
    if df is None or len(df) == 0:
        logger.error("Nenhum dado foi processado!")
        logger.error("Verifique se existem arquivos ODS em dados_ida/")
        return None
    
    logger.info(f"Total de registros processados: {len(df)}")
    return df


def carregar_staging(df):
    """Carrega dados na tabela staging."""
    logger.info("\n" + "="*80)
    logger.info("PASSO 2: CARREGAR DADOS NA STAGING")
    logger.info("="*80)
    
    loader = StagingLoader(DB_CONFIG)
    loader.connect()
    
    try:
        count = loader.load_to_staging(df, truncate=True)
        logger.info(f"Dados carregados na staging: {count} registros")
        
        # Verificar
        staging_count = loader.get_staging_count()
        logger.info(f"Total na staging: {staging_count} registros")
        
        return count > 0
    finally:
        loader.close()


def executar_transformacoes():
    """Executa script SQL de transformacoes."""
    logger.info("\n" + "="*80)
    logger.info("PASSO 3: TRANSFORMACOES SQL")
    logger.info("="*80)
    logger.info("As transformacoes SQL serao executadas pelo container...")
    return True


def verificar_dados():
    """Verifica dados carregados no Data Mart."""
    logger.info("\n" + "="*80)
    logger.info("PASSO 4: VERIFICAR DADOS CARREGADOS")
    logger.info("="*80)
    
    import psycopg2
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO ida, public")
    
    try:
        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM staging_ida")
        count_staging = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dim_tempo")
        count_tempo = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dim_grupo_economico")
        count_grupos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM fato_ida")
        count_fato = cursor.fetchone()[0]
        
        logger.info(f"Staging: {count_staging} registros")
        logger.info(f"Dimensao Tempo: {count_tempo} registros")
        logger.info(f"Dimensao Grupos: {count_grupos} registros")
        logger.info(f"Fato IDA: {count_fato} registros")
        
        if count_fato > 0:
            logger.info("\nPrimeiros registros da fato_ida:")
            cursor.execute("""
                SELECT 
                    dt.ano_mes,
                    dg.nome_grupo,
                    ds.codigo_servico,
                    f.taxa_resolvidas_5dias,
                    f.total_solicitacoes
                FROM fato_ida f
                JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
                JOIN dim_grupo_economico dg ON f.id_grupo = dg.id_grupo
                JOIN dim_servico ds ON f.id_servico = ds.id_servico
                LIMIT 5
            """)
            
            for row in cursor.fetchall():
                logger.info(f"  {row[0]} | {row[1]} | {row[2]} | Taxa: {row[3]}% | Solicitacoes: {row[4]}")
        
    finally:
        cursor.close()
        conn.close()


def main():
    """Funcao principal."""
    logger.info("="*80)
    logger.info("CARREGAR DADOS NO DATA MART IDA")
    logger.info("="*80)
    
    try:
        # Aguardar PostgreSQL
        if not aguardar_postgres():
            return False
        
        # Processar ODS
        df = processar_ods()
        if df is None:
            return False
        
        # Carregar staging
        if not carregar_staging(df):
            return False
        
        # Executar transformacoes
        if not executar_transformacoes():
            return False
        
        # Verificar dados
        verificar_dados()
        
        logger.info("\n" + "="*80)
        logger.info("CARGA CONCLUIDA COM SUCESSO!")
        logger.info("="*80)
        logger.info("")
        logger.info("Acesse o PostgreSQL:")
        logger.info("  docker exec -it ida_postgres psql -U postgres -d ida_datamart")
        logger.info("")
        logger.info("Consultas uteis:")
        logger.info("  SELECT * FROM ida.fato_ida LIMIT 10;")
        logger.info("  SELECT * FROM ida.vw_taxa_variacao_mensal LIMIT 10;")
        logger.info("="*80)
        
        return True
        
    except Exception as e:
        logger.error(f"Erro no processo: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    sucesso = main()
    sys.exit(0 if sucesso else 1)
