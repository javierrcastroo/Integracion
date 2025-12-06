#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "== Proyecto vuelos en streaming =="

# 1️⃣ Crear entorno virtual si no existe
if [ ! -d "venv" ]; then
  echo "[INFO] Creando entorno virtual..."
  python3 -m venv venv
fi

echo "[INFO] Activando entorno virtual..."
source venv/bin/activate

# 2️⃣ Instalar dependencias si faltan
echo "[INFO] Comprobando dependencias..."
pip install -r requirements.txt >/dev/null 2>&1 || {
  echo "[INFO] Instalando dependencias..."
  pip install -r requirements.txt
}

# 3️⃣ Levantar infraestructura Docker
echo "[INFO] Iniciando contenedores (Kafka, MongoDB, UIs)..."
docker compose up -d

# 4️⃣ Dar tiempo a Kafka y Mongo para arrancar
echo "[INFO] Esperando a que la infraestructura esté lista..."
sleep 25

# 5️⃣ Producer en background
echo "[INFO] Lanzando Producer..."
python src/flights_producer.py &
PRODUCER_PID=$!

sleep 3

# 6️⃣ Consumer en background
echo "[INFO] Lanzando Consumer..."
python src/flights_consumer.py &
CONSUMER_PID=$!

sleep 3

# 7️⃣ Dashboard Streamlit (foreground)
echo "[INFO] Lanzando Dashboard (http://localhost:8501)..."
streamlit run streamlit_app.py

# 8️⃣ Al cerrar Streamlit → matar producer/consumer
echo "[INFO] Finalizando Producer/Consumer..."
kill $PRODUCER_PID $CONSUMER_PID 2>/dev/null || true

echo "[INFO] Pipeline finalizado correctamente."
