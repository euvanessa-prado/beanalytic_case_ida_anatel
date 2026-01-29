"""Orquestrador de ETL para o Data Mart IDA da Anatel.

Coordena a inicialização do esquema, ingestão ODS, normalização e
transformações analíticas, produzindo tabelas dimensionais e views.
"""

import sys
import time
import logging
import psycopg2
import re
import pandas as pd
from pathlib import Path
from src.ods_processor import DataNormalizer, ODSProcessor
from src.staging_loader import StagingManager
from src.config import DB_CONFIG, DATA_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s')
logger = logging.getLogger("Pipeline")

class ETLPipeline:
    """Pipeline de ETL ponta a ponta para o conjunto de dados IDA."""

    def __init__(self):
        """Inicializa o pipeline com o caminho do diretório SQL."""
        self.sql_path = Path(__file__).parent / "sql"
        
    def _wait_for_db(self) -> bool:
        """Aguarda a disponibilidade do banco de dados (healthcheck) por ~30s."""
        for _ in range(15):
            try:
                with psycopg2.connect(**DB_CONFIG) as conn:
                    return True
            except:
                time.sleep(2)
        return False

    def _execute_sql_file(self, filename: str):
        """Executa um script SQL no Postgres (schemas ida/public).
        
        Args:
            filename: Nome do arquivo relativo ao diretório sql.
        """
        path = self.sql_path / filename
        with psycopg2.connect(**DB_CONFIG, options='-c search_path=ida,public') as conn:
            with conn.cursor() as cur:
                cur.execute(path.read_text())
            conn.commit()

    def run(self):
        """Executa o pipeline completo: DDL → Staging → Transformação → Views."""
        if not self._wait_for_db():
            logger.error("Timeout na conexão com o banco de dados.")
            sys.exit(1)

        try:
            # 1. Inicializar esquema
            self._execute_sql_file("00_init_completo.sql")

            # 2. Ingestão sequencial de arquivos (com exportação Parquet)
            processor = ODSProcessor(DATA_DIR)
            # Processa tudo de uma vez (usando Polars internamente e salvando parquet)
            # Retorna DF concatenado para carga no banco (mantendo compatibilidade)
            df_full = processor.process_all(export_parquet=True)
            
            if df_full.empty:
                logger.warning("Nenhum dado processado.")
                return

            loader = StagingManager(DB_CONFIG)
            
            # Limpeza total da área de staging
            with psycopg2.connect(**DB_CONFIG, options='-c search_path=ida,public') as conn:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE staging_ida RESTART IDENTITY")
                conn.commit()

            # Carga em lote (Staging)
            # Pequeno ajuste: ODSProcessor já retorna tudo concatenado, mas vamos garantir filtros finais
            if not df_full.empty:
                 # Controle de qualidade final
                 df_full = df_full[df_full['valor'] >= 0]
                 
                 logger.info(f"Iniciando carga no banco de {len(df_full)} registros...")
                 loader.bulk_load(df_full, truncate=False)
                 total_records = len(df_full)

            # 3. Transformações finais
            if total_records > 0:
                self._execute_sql_file("01_transform_load.sql")
                self._execute_sql_file("view_taxa_resolucao_5_dias.sql")
                logger.info(f"ETL concluído com sucesso. Registros processados: {total_records}")
            
        except Exception:
            logger.exception("Falha na execução do pipeline.")
            sys.exit(1)

if __name__ == "__main__":
    ETLPipeline().run()
