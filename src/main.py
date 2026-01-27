"""
Pipeline ETL principal para Data Mart IDA.

Este módulo implementa o pipeline completo de ETL (Extract, Transform, Load)
para processar dados de desempenho no atendimento de operadoras e carregar
no Data Mart PostgreSQL.

Classes:
    ETLPipeline: Orquestrador do processo ETL completo

Exemplo de uso:
    $ python src/main.py
"""

import logging
import time
from typing import Optional
import pandas as pd
from config import DB_CONFIG, DATA_DIR
from ods_processor import ODSProcessor
from loader import DataLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Pipeline ETL completo para Data Mart IDA.
    
    Esta classe orquestra o processo completo de extração, transformação
    e carga de dados no Data Mart PostgreSQL.
    
    Attributes:
        db_config (dict): Configurações de conexão com banco
        data_dir (str): Diretório com arquivos ODS
        processor (ODSProcessor): Processador de arquivos ODS
        loader (DataLoader): Carregador de dados no PostgreSQL
    
    Example:
        >>> pipeline = ETLPipeline(DB_CONFIG, DATA_DIR)
        >>> pipeline.executar()
    """
    
    def __init__(self, db_config: dict, data_dir: str):
        """
        Inicializa o pipeline ETL.
        
        Args:
            db_config (dict): Configurações do banco de dados
            data_dir (str): Diretório contendo arquivos ODS
        """
        self.db_config = db_config
        self.data_dir = data_dir
        self.processor = ODSProcessor(data_dir)
        self.loader = DataLoader(db_config)
    
    def extrair_transformar(self) -> Optional[pd.DataFrame]:
        """
        Fase de extração e transformação (Extract & Transform).
        
        Processa arquivos ODS do diretório, realizando leitura e
        normalização dos dados.
        
        Returns:
            pd.DataFrame: DataFrame com dados processados, ou None se falhar
        """
        logger.info("="*80)
        logger.info("FASE 1: EXTRAÇÃO E TRANSFORMAÇÃO")
        logger.info("="*80)
        
        logger.info(f"Processando arquivos ODS da pasta {self.data_dir}/...")
        df = self.processor.processar_todos()
        
        if df is None or len(df) == 0:
            logger.error("Nenhum dado foi processado!")
            logger.info("Execute primeiro: python baixar_dinamico.py")
            return None
        
        logger.info(f"Dados processados: {len(df)} registros")
        return df
    
    def carregar(self, df: pd.DataFrame) -> bool:
        """
        Fase de carga (Load).
        
        Carrega dados processados no Data Mart PostgreSQL.
        
        Args:
            df (pd.DataFrame): DataFrame com dados a carregar
        
        Returns:
            bool: True se carga foi bem-sucedida, False caso contrário
        """
        logger.info("\n" + "="*80)
        logger.info("FASE 2: CARGA NO DATA MART")
        logger.info("="*80)
        
        try:
            self.loader.connect()
            self.loader.load_all(df)
            return True
        except Exception as e:
            logger.error(f"Erro na carga: {e}")
            return False
        finally:
            self.loader.close()
    
    def executar(self) -> bool:
        """
        Executa pipeline ETL completo.
        
        Coordena as fases de extração, transformação e carga,
        exibindo informações de progresso e resultado.
        
        Returns:
            bool: True se pipeline foi executado com sucesso
        """
        self._exibir_cabecalho()
        
        try:
            # Aguardar PostgreSQL estar pronto
            logger.info("Aguardando PostgreSQL...")
            time.sleep(10)
            
            # Fase 1: Extract & Transform
            df = self.extrair_transformar()
            if df is None:
                return False
            
            # Fase 2: Load
            sucesso = self.carregar(df)
            
            if sucesso:
                self._exibir_sucesso()
                return True
            else:
                return False
            
        except Exception as e:
            logger.error(f"Erro no pipeline: {e}")
            raise
    
    def _exibir_cabecalho(self) -> None:
        """Exibe cabeçalho do pipeline."""
        logger.info("="*80)
        logger.info("PIPELINE ETL - DATA MART IDA")
        logger.info("="*80)
    
    def _exibir_sucesso(self) -> None:
        """Exibe mensagem de sucesso e instruções."""
        logger.info("\n" + "="*80)
        logger.info("PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info("="*80)
        logger.info("")
        logger.info("Acesse o PostgreSQL:")
        logger.info(f"  Host: {self.db_config['host']}")
        logger.info(f"  Port: {self.db_config['port']}")
        logger.info(f"  Database: {self.db_config['database']}")
        logger.info(f"  User: {self.db_config['user']}")
        logger.info(f"  Password: {self.db_config['password']}")
        logger.info("")
        logger.info("Consulte a view:")
        logger.info("  SELECT * FROM ida.vw_taxa_variacao_mensal;")
        logger.info("="*80)


def main():
    """
    Função principal do script.
    
    Cria uma instância do ETLPipeline e executa o processo completo.
    """
    pipeline = ETLPipeline(DB_CONFIG, DATA_DIR)
    pipeline.executar()


if __name__ == '__main__':
    main()
