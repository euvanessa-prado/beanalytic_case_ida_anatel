"""ETL Orchestrator for the Anatel IDA Data Mart.

Coordinates schema initialization, ODS ingestion, normalization and
analytical transformations, producing dimensional tables and views.
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
    """End-to-end ETL pipeline for the IDA dataset."""

    def __init__(self):
        """Initialize pipeline with SQL directory path."""
        self.sql_path = Path(__file__).parent / "sql"
        
    def _wait_for_db(self) -> bool:
        """Wait for database readiness (healthcheck) up to ~30s."""
        for _ in range(15):
            try:
                with psycopg2.connect(**DB_CONFIG) as conn:
                    return True
            except:
                time.sleep(2)
        return False

    def _execute_sql_file(self, filename: str):
        """Execute a SQL script file against Postgres in ida/public.
        
        Args:
            filename: File name relative to the sql directory.
        """
        path = self.sql_path / filename
        with psycopg2.connect(**DB_CONFIG, options='-c search_path=ida,public') as conn:
            with conn.cursor() as cur:
                cur.execute(path.read_text())
            conn.commit()

    def run(self):
        """Run the full pipeline: DDL → Staging → Transform → Views."""
        if not self._wait_for_db():
            logger.error("Database connection timeout.")
            sys.exit(1)

        try:
            # 1. Initialize schema
            self._execute_sql_file("00_init_completo.sql")

            # 2. Sequential file ingestion
            files = list(Path(DATA_DIR).glob('*.ods'))
            if not files:
                logger.warning("No .ods files found.")
                return

            loader = StagingManager(DB_CONFIG)
            
            # Pure reset of staging area
            with psycopg2.connect(**DB_CONFIG, options='-c search_path=ida,public') as conn:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE staging_ida RESTART IDENTITY")
                conn.commit()

            total_records = 0
            for f in files:
                logger.info(f"Ingesting: {f.name}")
                
                # Metadata extraction
                svc = re.sub(r'\d+', '', f.stem).upper()
                yr_match = re.search(r'\d{4}', f.stem)
                yr = int(yr_match.group()) if yr_match else None
                
                # Load and normalize
                df = DataNormalizer(target_year=yr).normalize(pd.read_excel(f, engine='odf'))
                
                if not df.empty:
                    # Quality control
                    df = df[df['valor'] >= 0]
                    df['servico'] = svc
                    df['arquivo_origem'] = f.name
                    df['ano_mes'] = df.apply(lambda r: f"{int(r.ano)}-{int(r.mes):02d}", axis=1)
                    
                    loader.bulk_load(df, truncate=False)
                    total_records += len(df)

            # 3. Final transformations
            if total_records > 0:
                self._execute_sql_file("01_transform_load.sql")
                self._execute_sql_file("02_view_pivotada.sql")
                logger.info(f"ETL completed successfully. Records processed: {total_records}")
            
        except Exception:
            logger.exception("Pipeline execution failed.")
            sys.exit(1)

if __name__ == "__main__":
    ETLPipeline().run()
