#!/bin/bash
set -e

echo "[RESET] Parando y borrando contenedores..."
docker compose down -v

echo "[RESET] Borrando CSV generados..."
rm -f data/flights_batch_*.csv || true

echo "[RESET] Borrando gráficas generadas..."
rm -rf report_plots || true

echo "[RESET] Hecho. Proyecto listo para arrancar de cero."
