import json
import re
from datetime import datetime, timezone
from typing import List, Dict

import pandas as pd
from kafka import KafkaConsumer
from pymongo import MongoClient

TOPIC = "flights_raw"
BATCH_SIZE_SAVE = 50  # procesamos de 50 en 50

# Columnas del CSV
COL_YEAR = "year"
COL_MONTH = "month"
COL_DAY_OF_MONTH = "day_of_month"
COL_DAY_OF_WEEK = "day_of_week"
COL_DATE = "fl_date"

COL_CARRIER = "op_unique_carrier"
COL_FLIGHT_NUM = "op_carrier_fl_num"
COL_ORIGIN = "origin"
COL_DEST = "dest"

COL_ARR_DELAY = "arr_delay"
COL_DISTANCE = "distance"
COL_CANCELLED = "cancelled"
COL_DIVERTED = "diverted"

# Config MongoDB
mongo_client = MongoClient("mongodb://javier:1234@localhost:27017/")
db = mongo_client["flights_db"]
COL_GOOD = db["good_flights_clean"]
COL_BAD = db["bad_flights_clean"]


def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
    )


# -------------------------
# 📌 Calidad del dato
# -------------------------
def apply_data_quality(df: pd.DataFrame):
    df = df.copy()

    required_cols = [
        COL_DATE, COL_ORIGIN, COL_DEST, COL_CARRIER,
        COL_FLIGHT_NUM, COL_ARR_DELAY, COL_DISTANCE
    ]
    df = df.dropna(subset=required_cols)

    df[COL_YEAR] = pd.to_numeric(df[COL_YEAR], errors="coerce")
    df = df[df[COL_YEAR] == 2024]

    df[COL_MONTH] = pd.to_numeric(df[COL_MONTH], errors="coerce")
    df = df[df[COL_MONTH].between(1, 12)]

    df[COL_DAY_OF_MONTH] = pd.to_numeric(df[COL_DAY_OF_MONTH], errors="coerce")
    df = df[df[COL_DAY_OF_MONTH].between(1, 31)]

    df[COL_DAY_OF_WEEK] = pd.to_numeric(df[COL_DAY_OF_WEEK], errors="coerce")
    df = df[df[COL_DAY_OF_WEEK].between(1, 7)]

    df[COL_ARR_DELAY] = pd.to_numeric(df[COL_ARR_DELAY], errors="coerce")
    df = df[df[COL_ARR_DELAY].between(-120, 1440)]

    df[COL_DISTANCE] = pd.to_numeric(df[COL_DISTANCE], errors="coerce")
    df = df[df[COL_DISTANCE] > 0]

    df[COL_CANCELLED] = pd.to_numeric(df[COL_CANCELLED], errors="coerce")
    df = df[df[COL_CANCELLED].isin([0, 1])]

    df[COL_DIVERTED] = pd.to_numeric(df[COL_DIVERTED], errors="coerce")
    df = df[df[COL_DIVERTED].isin([0, 1])]

    pattern_iata = re.compile(r"^[A-Z]{3}$")

    df = df[df[COL_ORIGIN].str.match(pattern_iata)]
    df = df[df[COL_DEST].str.match(pattern_iata)]

    df = df.drop_duplicates(
        subset=[COL_DATE, COL_CARRIER, COL_FLIGHT_NUM, COL_ORIGIN, COL_DEST]
    )

    df[COL_DATE] = pd.to_datetime(df[COL_DATE], errors="coerce")
    df = df[(df[COL_DATE] >= "2024-01-01") & (df[COL_DATE] <= "2024-12-31")]

    return df


# -------------------------
# 📌 Filtrado retrasos
# -------------------------
def is_good_flight(delay):
    return abs(delay) <= 15


# -------------------------
# 🔥 PROCESAMIENTO COMPLETO
# -------------------------
def process_batch(records):
    if not records:
        return

    df = pd.DataFrame(records)
    print(f"[BATCH] Recibidas {len(df)} filas crudas")

    df_quality = apply_data_quality(df)
    print(f"[BATCH] Calidad del dato OK: {len(df_quality)} filas")

    ts = datetime.now(timezone.utc)

    for _, row in df_quality.iterrows():
        doc = row.to_dict()
        doc["_ingestion_ts_utc"] = ts

        if is_good_flight(row[COL_ARR_DELAY]):
            COL_GOOD.insert_one(doc)
        else:
            COL_BAD.insert_one(doc)

    print(f"[BATCH] Guardado: BUENOS={df_quality[COL_ARR_DELAY].abs().le(15).sum()} "
          f"| MALOS={df_quality[COL_ARR_DELAY].abs().gt(15).sum()}")


def main():
    consumer = create_consumer()
    buffer: List[Dict] = []

    print("[CONSUMER] Esperando datos...")

    for msg in consumer:
        buffer.append(msg.value)

        if len(buffer) >= BATCH_SIZE_SAVE:
            process_batch(buffer)
            buffer.clear()


if __name__ == "__main__":
    main()
