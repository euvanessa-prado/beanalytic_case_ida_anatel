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
from src.ods_processor import DataNormalizer
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

            # 2. Ingestão sequencial de arquivos
            files = list(Path(DATA_DIR).glob('*.ods'))
            if not files:
                logger.warning("Nenhum arquivo .ods encontrado.")
                return

            loader = StagingManager(DB_CONFIG)
            
            # Limpeza total da área de staging
            with psycopg2.connect(**DB_CONFIG, options='-c search_path=ida,public') as conn:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE staging_ida RESTART IDENTITY")
                conn.commit()

            total_records = 0
            for f in files:
                logger.info(f"Ingerindo: {f.name}")
                
                # Extração de metadados
                svc = re.sub(r'\d+', '', f.stem).upper()
                yr_match = re.search(r'\d{4}', f.stem)
                yr = int(yr_match.group()) if yr_match else None
                
                # Carga e normalização
                df = DataNormalizer(target_year=yr).normalize(pd.read_excel(f, engine='odf'))
                
                if not df.empty:
                    # Controle de qualidade
                    df = df[df['valor'] >= 0]
                    df['servico'] = svc
                    df['arquivo_origem'] = f.name
                    df['ano_mes'] = df.apply(lambda r: f"{int(r.ano)}-{int(r.mes):02d}", axis=1)
                    
                    loader.bulk_load(df, truncate=False)
                    total_records += len(df)

            # 3. Transformações finais
            if total_records > 0:
                self._execute_sql_file("01_transform_load.sql")
                self._execute_sql_file("02_view_pivotada.sql")
                logger.info(f"ETL concluído com sucesso. Registros processados: {total_records}")
            
        except Exception:
            logger.exception("Falha na execução do pipeline.")
            sys.exit(1)

if __name__ == "__main__":
    ETLPipeline().run()
