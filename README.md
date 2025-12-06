# Proyecto - Ingesta y Análisis de Vuelos en Streaming

Este proyecto simula un pipeline de datos en tiempo real para procesar información de vuelos.

Flujo de datos:
CSV → Producer (Python) → Kafka → Consumer (Python) → MongoDB → Streamlit (Dashboard)

## Requisitos previos

- Docker y Docker Compose instalados
- Python 3.8 o superior

## Instalación

Clonar el proyecto y entrar a la carpeta:

```bash
cd BIGDATA/practica1
```

Crear entorno virtual e instalar dependencias:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Ejecución

1. Levantar infraestructura:

```bash
docker compose up -d
```

2. Ejecutar el productor (envía datos cada 20 segundos):

```bash
source venv/bin/activate
python src/flights_producer.py
```

3. Ejecutar el consumidor (limpia y clasifica datos):

```bash
source venv/bin/activate
python src/flights_consumer.py
```

4. Ejecutar el dashboard:

```bash
source venv/bin/activate
streamlit run streamlit_app.py
```

La aplicación web estará en:
http://localhost:8501

## Servicios disponibles

Kafka UI:
http://localhost:8080

Mongo Express:
http://localhost:8081

## Estructura del proyecto

```
practica1/
├─ src/
│  ├─ flights_producer.py
│  └─ flights_consumer.py
├─ streamlit_app.py
├─ requirements.txt
├─ docker-compose.yml
└─ README.md
```

## Reiniciar el sistema (borrar datos y contenedores)

```bash
docker compose down -v
rm -f data/flights_batch_*.csv
rm -rf report_plots
```

Fin del documento.
