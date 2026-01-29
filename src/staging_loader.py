"""Carregador de alta performance para a camada de staging (PostgreSQL).

Fornece uma abstração sobre o psycopg2 para inserir linhas normalizadas em lote
na tabela ida.staging_ida usando execute_values para velocidade.
"""

import logging
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

logger = logging.getLogger(__name__)

class StagingManager:
    """Gerenciador de carga em lote para a tabela ida.staging_ida."""

    def __init__(self, db_config: dict):
        """Inicializa com credenciais do banco de dados (psycopg2)."""
        self.db_config = db_config

    def bulk_load(self, df: pd.DataFrame, truncate: bool = True):
        """Insere dados normalizados em lote na staging_ida.
        
        Args:
            df: DataFrame contendo os dados normalizados.
            truncate: Se True, limpa a tabela de staging antes da carga.
        """
        if df.empty:
            return

        with psycopg2.connect(**self.db_config, options='-c search_path=ida,public') as conn:
            with conn.cursor() as cur:
                if truncate:
                    cur.execute("TRUNCATE TABLE staging_ida RESTART IDENTITY CASCADE")
                
                cols = ['ano', 'mes', 'ano_mes', 'servico', 'grupo_economico', 'variavel', 'valor', 'arquivo_origem']
                data = [tuple(x) for x in df[cols].to_numpy()]

                sql = f"INSERT INTO staging_ida ({','.join(cols)}) VALUES %s"
                execute_values(cur, sql, data)
            conn.commit()
            
        logger.info(f"Carregados {len(df)} registros na camada de staging.")
