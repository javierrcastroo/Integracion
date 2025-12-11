#!/usr/bin/env bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

if [ -z "$BASH_VERSION" ]; then
  echo "[ERROR] Ejecuta este script con bash (ej. 'bash run_all.sh')." >&2
  exit 1
fi

echo "== Proyecto vuelos en streaming =="

# 1️⃣ Verificar binario de Python y entorno virtual
PYTHON_BIN="$(command -v python3 || true)"
if [ -z "$PYTHON_BIN" ]; then
  echo "[ERROR] No se encontró python3 en el PATH. Instálalo antes de continuar." >&2
  exit 1
fi

VENV_DIR="${PROJECT_DIR}/venv"
if [ ! -d "$VENV_DIR" ]; then
  echo "[INFO] Creando entorno virtual en $VENV_DIR..."
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

echo "[INFO] Activando entorno virtual..."
source "$VENV_DIR/bin/activate"
echo "[INFO] Usando Python en: $(command -v python)"
echo "[INFO] Usando pip en:    $(command -v pip)"

# 2️⃣ Instalar dependencias de forma explícita
echo "[INFO] Instalando/actualizando dependencias..."
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# Verificación rápida de dependencias críticas
python - <<'PY'
import importlib
modules = ["pandas", "kafka", "pymongo", "streamlit"]
missing = []
for mod in modules:
    try:
        importlib.import_module(mod)
    except Exception as exc:  # noqa: BLE001 - mostrar causa al usuario
        missing.append((mod, exc))

if missing:
    print("[ERROR] Faltan dependencias tras la instalación:")
    for mod, exc in missing:
        print(f"  - {mod}: {exc}")
    print("Revisa la salida de pip e intenta nuevamente.")
    raise SystemExit(1)
PY

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
