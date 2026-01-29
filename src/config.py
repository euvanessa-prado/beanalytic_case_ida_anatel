import os
from pathlib import Path
from dotenv import load_dotenv

# Carrega .env se existir
load_dotenv()

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = os.getenv("DATA_DIR", "dados_ida")

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "ida_datamart"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}
