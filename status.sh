#!/usr/bin/env bash
set -euo pipefail
echo "Validando Data Mart (schema ida) ..."
docker compose exec -T postgres psql -U postgres -d ida_datamart -c "SET search_path TO ida, public; SELECT tablename FROM pg_tables WHERE schemaname='ida' ORDER BY 1;"
docker compose exec -T postgres psql -U postgres -d ida_datamart -c "SELECT viewname FROM pg_views WHERE schemaname='ida' ORDER BY 1;"
echo
echo "Contagens:"
docker compose exec -T postgres psql -U postgres -d ida_datamart -c "SELECT COUNT(*) AS staging_rows FROM ida.staging_ida;"
docker compose exec -T postgres psql -U postgres -d ida_datamart -c "SELECT COUNT(*) AS fato_rows FROM ida.fato_ida;"
docker compose exec -T postgres psql -U postgres -d ida_datamart -c "SELECT COUNT(*) AS view_rows FROM ida.view_taxa_resolucao_5_dias;"
