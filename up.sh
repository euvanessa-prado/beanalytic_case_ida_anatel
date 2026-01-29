#!/usr/bin/env bash
set -euo pipefail
echo "Resetando volumes e subindo a stack..."
docker compose down -v
docker compose up -d
echo "Serviços:"
docker compose ps
echo
echo "Acompanhe os logs do ETL (Ctrl+C para sair):"
docker compose logs -f data_loader
