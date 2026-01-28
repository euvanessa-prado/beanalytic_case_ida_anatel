"""
Modulo de carga de dados brutos na tabela staging.

Este modulo carrega dados normalizados dos arquivos ODS diretamente
na tabela staging do PostgreSQL, sem transformacoes.

Classes:
    StagingLoader: Carregador de dados na tabela staging

Exemplo de uso:
    >>> from staging_loader import StagingLoader
    >>> from config import DB_CONFIG
    >>> loader = StagingLoader(DB_CONFIG)
    >>> loader.load_to_staging(df)
"""

import psycopg2
import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)


class StagingLoader:
    """
    Carregador de dados na tabela staging.
    
    Esta classe carrega dados brutos normalizados diretamente na
    tabela staging do PostgreSQL.
    
    Attributes:
        db_config (dict): Configuracoes de conexao
        conn: Conexao com banco de dados
        cursor: Cursor do banco de dados
    """
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Inicializa o loader.
        
        Args:
            db_config (dict): Dicionario com configuracoes do banco
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None
    
    def connect(self) -> None:
        """Conecta ao banco de dados."""
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()
        self.cursor.execute("SET search_path TO ida, public")
        logger.info("Conectado ao PostgreSQL")
    
    def close(self) -> None:
        """Fecha conexao com o banco de dados."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Conexao fechada")
    
    def truncate_staging(self) -> None:
        """Limpa a tabela staging antes de carregar novos dados."""
        self.cursor.execute("TRUNCATE TABLE staging_ida RESTART IDENTITY CASCADE")
        self.conn.commit()
        logger.info("Tabela staging limpa")
    
    def load_to_staging(self, df: pd.DataFrame, truncate: bool = True) -> int:
        """
        Carrega dados na tabela staging.
        
        Args:
            df (pd.DataFrame): DataFrame com dados normalizados
                              Colunas: ano, mes, ano_mes, servico, 
                                      grupo_economico, variavel, valor
            truncate (bool): Se True, limpa a staging antes de carregar
        
        Returns:
            int: Numero de registros carregados
        """
        if truncate:
            self.truncate_staging()
        
        count = 0
        batch_size = 1000
        
        logger.info(f"Carregando {len(df)} registros na staging...")
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            for _, row in batch.iterrows():
                try:
                    self.cursor.execute("""
                        INSERT INTO staging_ida (
                            ano, mes, ano_mes, servico, grupo_economico, 
                            variavel, valor, arquivo_origem
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row['ano'],
                        row['mes'],
                        row['ano_mes'],
                        row['servico'],
                        row['grupo_economico'],
                        row['variavel'],
                        row['valor'],
                        row.get('arquivo', 'unknown')
                    ))
                    count += 1
                except Exception as e:
                    logger.error(f"Erro ao inserir registro: {e}")
                    logger.error(f"Dados: {row.to_dict()}")
                    continue
            
            self.conn.commit()
            logger.info(f"Carregados {count} registros...")
        
        logger.info(f"Carga na staging concluida: {count} registros")
        return count
    
    def get_staging_count(self) -> int:
        """Retorna o numero de registros na staging."""
        self.cursor.execute("SELECT COUNT(*) FROM staging_ida")
        return self.cursor.fetchone()[0]
