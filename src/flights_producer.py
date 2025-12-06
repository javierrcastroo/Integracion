import time
import json
import random
from typing import List, Dict

import pandas as pd
from kafka import KafkaProducer

CSV_PATH = "data/flights.csv"   # ruta relativa desde donde ejecutes el script
TOPIC = "flights_raw"

BATCH_SIZE = 10       # número de filas por “micro-lote”
SLEEP_SECONDS = 5    # cada cuántos segundos mandamos otro lote


def create_producer() -> KafkaProducer:
    """
    Crea un productor Kafka que serializa los mensajes como JSON.
    """
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",   # porque estás fuera de Docker
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: str(v).encode("utf-8") if v is not None else None,
    )
    return producer


def load_flights(csv_path: str) -> pd.DataFrame:
    """
    Carga el CSV de vuelos en un DataFrame.
    """
    print(f"[PRODUCER] Cargando CSV desde {csv_path}...")
    df = pd.read_csv(csv_path)
    print(f"[PRODUCER] CSV cargado con {len(df)} filas.")
    return df


def sample_batch(df: pd.DataFrame, n: int) -> List[Dict]:
    """
    Devuelve n filas aleatorias del DataFrame como lista de diccionarios.
    """
    sample = df.sample(n=n)
    return sample.to_dict(orient="records")


def main():
    df = load_flights(CSV_PATH)
    producer = create_producer()
    print("[PRODUCER] Conectado a Kafka. Empezando a enviar micro-lotes...")

    while True:
        batch_id = random.randint(0, 10**9)
        records = sample_batch(df, BATCH_SIZE)

        print(f"[PRODUCER] Enviando batch {batch_id} con {len(records)} mensajes...")

        for i, row in enumerate(records):
            key = f"{batch_id}_{i}"
            producer.send(TOPIC, key=key, value=row)

        producer.flush()
        print(f"[PRODUCER] Batch {batch_id} enviado. Esperando {SLEEP_SECONDS} segundos...\n")
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()

