import os
from typing import Tuple

import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient

MONGO_URI = "mongodb://root:example@localhost:27017/"
DB_NAME = "flights_db"
GOOD_COLL = "good_flights_clean"
BAD_COLL = "bad_flights_clean"

OUTPUT_DIR = "report_plots"


def load_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Carga datos de MongoDB en dos DataFrames: good y bad."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    good = pd.DataFrame(list(db[GOOD_COLL].find()))
    bad = pd.DataFrame(list(db[BAD_COLL].find()))

    # Por si vienen columnas raras de Mongo
    for df in (good, bad):
        if "fl_date" in df.columns:
            df["fl_date"] = pd.to_datetime(df["fl_date"], errors="coerce")
        if "month" in df.columns:
            df["month"] = pd.to_numeric(df["month"], errors="coerce")
        if "day_of_week" in df.columns:
            df["day_of_week"] = pd.to_numeric(df["day_of_week"], errors="coerce")
        if "arr_delay" in df.columns:
            df["arr_delay"] = pd.to_numeric(df["arr_delay"], errors="coerce")

    return good, bad


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def save_current_fig(name: str):
    """Guarda la figura actual en la carpeta OUTPUT_DIR con nombre name.png."""
    path = os.path.join(OUTPUT_DIR, f"{name}.png")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    print(f"[REPORT] Figura guardada en {path}")


def plot_good_vs_bad_by_origin(good: pd.DataFrame, bad: pd.DataFrame, top_n: int = 10):
    """Top N aeropuertos de origen: buenos vs malos."""
    good_counts = good["origin"].value_counts().head(top_n)
    bad_counts = bad["origin"].value_counts().head(top_n)

    # Unimos las dos series en un DataFrame
    df = pd.concat(
        [
            good_counts.rename("good"),
            bad_counts.rename("bad"),
        ],
        axis=1,
    ).fillna(0)

    ax = df.plot(kind="bar")
    ax.set_title(f"Top {top_n} aeropuertos de origen (vuelos buenos vs malos)")
    ax.set_xlabel("Aeropuerto de origen")
    ax.set_ylabel("Número de vuelos")

    save_current_fig("01_top_origins_good_vs_bad")


def plot_monthly_counts(good: pd.DataFrame, bad: pd.DataFrame):
    """Número de vuelos buenos y malos por mes."""
    good_month = good.groupby("month")["_id"].count().rename("good")
    bad_month = bad.groupby("month")["_id"].count().rename("bad")

    df = pd.concat([good_month, bad_month], axis=1).fillna(0).sort_index()

    ax = df.plot(kind="line", marker="o")
    ax.set_title("Número de vuelos por mes (buenos vs malos)")
    ax.set_xlabel("Mes")
    ax.set_ylabel("Número de vuelos")

    save_current_fig("02_monthly_counts_good_vs_bad")


def plot_dayofweek_counts(good: pd.DataFrame, bad: pd.DataFrame):
    """Número de vuelos buenos y malos por día de la semana."""
    good_dow = good.groupby("day_of_week")["_id"].count().rename("good")
    bad_dow = bad.groupby("day_of_week")["_id"].count().rename("bad")

    df = pd.concat([good_dow, bad_dow], axis=1).fillna(0).sort_index()

    ax = df.plot(kind="bar")
    ax.set_title("Número de vuelos por día de la semana (buenos vs malos)")
    ax.set_xlabel("Día de la semana (1=Lunes, 7=Domingo)")
    ax.set_ylabel("Número de vuelos")

    save_current_fig("03_dayofweek_counts_good_vs_bad")


def plot_delay_histograms(good: pd.DataFrame, bad: pd.DataFrame):
    """Histogramas de retraso de llegada para vuelos buenos y malos."""
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))

    good_delays = good["arr_delay"].dropna()
    bad_delays = bad["arr_delay"].dropna()

    axes[0].hist(good_delays, bins=20)
    axes[0].set_title("Vuelos BUENOS - arr_delay")
    axes[0].set_xlabel("Retraso (min)")
    axes[0].set_ylabel("Frecuencia")

    axes[1].hist(bad_delays, bins=20)
    axes[1].set_title("Vuelos MALOS - arr_delay")
    axes[1].set_xlabel("Retraso (min)")
    axes[1].set_ylabel("Frecuencia")

    save_current_fig("04_histograms_arr_delay_good_vs_bad")


def plot_carrier_counts(good: pd.DataFrame, bad: pd.DataFrame, top_n: int = 10):
    """Top N aerolíneas por número de vuelos buenos vs malos."""
    good_counts = good["op_unique_carrier"].value_counts().head(top_n)
    bad_counts = bad["op_unique_carrier"].value_counts().head(top_n)

    df = pd.concat(
        [
            good_counts.rename("good"),
            bad_counts.rename("bad"),
        ],
        axis=1,
    ).fillna(0)

    ax = df.plot(kind="bar")
    ax.set_title(f"Top {top_n} aerolíneas (vuelos buenos vs malos)")
    ax.set_xlabel("Aerolínea")
    ax.set_ylabel("Número de vuelos")

    save_current_fig("05_carrier_counts_good_vs_bad")


def plot_top_routes(good: pd.DataFrame, bad: pd.DataFrame, top_n: int = 10):
    """Top N rutas (origen-destino) buenas vs malas."""
    good_routes = (
        good.assign(route=good["origin"] + "-" + good["dest"])
        .groupby("route")["_id"]
        .count()
        .sort_values(ascending=False)
        .head(top_n)
        .rename("good")
    )
    bad_routes = (
        bad.assign(route=bad["origin"] + "-" + bad["dest"])
        .groupby("route")["_id"]
        .count()
        .sort_values(ascending=False)
        .head(top_n)
        .rename("bad")
    )

    df = pd.concat([good_routes, bad_routes], axis=1).fillna(0)

    ax = df.plot(kind="bar")
    ax.set_title(f"Top {top_n} rutas (vuelos buenos vs malos)")
    ax.set_xlabel("Ruta origen-destino")
    ax.set_ylabel("Número de vuelos")

    save_current_fig("06_top_routes_good_vs_bad")


def main():
    ensure_output_dir()
    print("[REPORT] Cargando datos desde MongoDB...")
    good, bad = load_data()
    print(f"[REPORT] Vuelos buenos: {len(good)}, vuelos malos: {len(bad)}")

    if good.empty and bad.empty:
        print("[REPORT] No hay datos en MongoDB. Asegúrate de que el consumer ha insertado vuelos.")
        return

    plot_good_vs_bad_by_origin(good, bad)
    plot_monthly_counts(good, bad)
    plot_dayofweek_counts(good, bad)
    plot_delay_histograms(good, bad)
    plot_carrier_counts(good, bad)
    plot_top_routes(good, bad)


if __name__ == "__main__":
    main()
