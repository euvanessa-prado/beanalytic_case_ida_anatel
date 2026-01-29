"""High-performance loader for the staging layer (PostgreSQL).

Provides a thin abstraction over psycopg2 to batch-insert normalized rows
into ida.staging_ida using execute_values for speed.
"""

import logging
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

logger = logging.getLogger(__name__)

class StagingManager:
    """Manager for bulk-loading normalized rows into ida.staging_ida."""

    def __init__(self, db_config: dict):
        """Initialize with database credentials (psycopg2)."""
        self.db_config = db_config

    def bulk_load(self, df: pd.DataFrame, truncate: bool = True):
        """Batch insert normalized data into staging_ida.
        
        Args:
            df: DataFrame containing normalized data.
            truncate: Whether to clear the staging table before load.
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
            
        logger.info(f"Loaded {len(df)} records into staging layer.")
