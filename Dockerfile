FROM python:3.11.12-bookworm

# Instala dependências de sistema para o psycopg2 e psql
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código
COPY . .

# Inicia o orquestrador
CMD ["python", "carregar_dados.py"]
