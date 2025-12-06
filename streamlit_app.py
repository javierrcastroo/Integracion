import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
import streamlit as st
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# ======================
# CONFIG
# ======================
MONGO_URI = "mongodb://javier:1234@localhost:27017/"
DB_NAME = "flights_db"
GOOD_COLL = "good_flights_clean"
BAD_COLL = "bad_flights_clean"

REFRESH_SECONDS = 10


# ======================
# LOAD DATA
# ======================
@st.cache_data(ttl=REFRESH_SECONDS)
def load_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    good = pd.DataFrame(list(db[GOOD_COLL].find()))
    bad = pd.DataFrame(list(db[BAD_COLL].find()))

    for df in (good, bad):
        if "fl_date" in df.columns:
            df["fl_date"] = pd.to_datetime(df["fl_date"], errors="coerce")
        for col in ("month", "day_of_week", "arr_delay", "distance"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

    return good, bad


# ======================
# HELPERS PLOT
# ======================
def plot_bar(df, title, xlabel, ylabel):
    fig, ax = plt.subplots()
    df.plot(kind="bar", ax=ax)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    st.pyplot(fig)


def plot_line(df, title, xlabel, ylabel):
    fig, ax = plt.subplots()
    df.plot(kind="line", marker="o", ax=ax)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    st.pyplot(fig)


def plot_hist(series, title, xlabel, ylabel):
    fig, ax = plt.subplots()
    ax.hist(series.dropna(), bins=20)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    st.pyplot(fig)


def plot_scatter(x, y, title, xlabel, ylabel):
    fig, ax = plt.subplots()
    ax.scatter(x, y)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    st.pyplot(fig)


# ======================
# MAIN APP
# ======================
def main():
    st.set_page_config(page_title="Flights Dashboard", layout="wide")

    # Auto-refresh
    st_autorefresh(interval=REFRESH_SECONDS * 1000, key="data_refresh")

    st.title("📊 Dashboard de Vuelos en Tiempo Real")

    st.markdown(
        """
Este panel muestra el resultado del pipeline en streaming:

- Los **vuelos BUENOS** son aquellos que cumplen las reglas de calidad del dato y tienen `|arr_delay| ≤ 15`.
- Los **vuelos MALOS** cumplen calidad, pero tienen `|arr_delay| > 15`.

Los datos se actualizan automáticamente conforme llegan nuevos micro-lotes desde Kafka.
"""
    )

    last_update = datetime.now().strftime("%H:%M:%S")
    st.caption(f"Última actualización: **{last_update}** (refresco cada {REFRESH_SECONDS} s)")

    good, bad = load_data()

    if good.empty and bad.empty:
        st.warning("No hay datos en MongoDB todavía. Ejecuta el producer y el consumer.")
        return

    # ======================
    # KPIs iniciales
    # ======================
    total = len(good) + len(bad)
    pct_good = (len(good) / total * 100) if total > 0 else 0.0
    mean_delay_good = good["arr_delay"].mean() if "arr_delay" in good.columns else None
    mean_delay_bad = bad["arr_delay"].mean() if "arr_delay" in bad.columns else None

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Vuelos BUENOS", len(good))
    col2.metric("Vuelos MALOS", len(bad))
    col3.metric("% BUENOS", f"{pct_good:.1f}%")
    if mean_delay_good is not None and mean_delay_bad is not None:
        col4.metric(
            "Retraso medio (buenos / malos)",
            f"{mean_delay_good:.1f} min / {mean_delay_bad:.1f} min",
        )

    st.markdown(
        """
Estos indicadores resumen el estado global del sistema:

- El **número de vuelos buenos y malos** da una idea de la calidad operacional.
- El **porcentaje de vuelos buenos** resume el rendimiento global.
- El **retraso medio** permite comparar el comportamiento de ambos grupos.
"""
    )

    # ======================
    # VOLÚMENES POR AEROPUERTO
    # ======================
    st.header("1. Volumen de vuelos por aeropuerto de origen")

    st.markdown(
        """
Este gráfico compara el número de vuelos BUENOS y MALOS por aeropuerto de origen.

- Sirve para identificar **hubs con mucho tráfico**.
- Permite ver qué aeropuertos concentran más problemas (más vuelos malos).
"""
    )

    if "origin" in good.columns and "origin" in bad.columns:
        good_orig = good["origin"].value_counts().head(10)
        bad_orig = bad["origin"].value_counts().head(10)
        df_orig = pd.concat(
            [good_orig.rename("good"), bad_orig.rename("bad")],
            axis=1,
        ).fillna(0)
        plot_bar(df_orig, "Top 10 aeropuertos de origen (buenos vs malos)", "Aeropuerto", "Número de vuelos")
    else:
        st.info("No se ha encontrado la columna 'origin' en los datos.")

    # ======================
    # VUELOS POR MES
    # ======================
    st.header("2. Vuelos por mes (estacionalidad)")

    st.markdown(
        """
Este gráfico muestra cuántos vuelos BUENOS y MALOS se registran cada mes.

- Permite detectar **patrones estacionales** (por ejemplo, más actividad en verano).
- También ayuda a ver si en ciertos meses aumenta la proporción de vuelos malos.
"""
    )

    if "month" in good.columns and "month" in bad.columns:
        gm = good.groupby("month")["_id"].count().rename("good")
        bm = bad.groupby("month")["_id"].count().rename("bad")
        df_m = pd.concat([gm, bm], axis=1).fillna(0).sort_index()
        plot_line(df_m, "Vuelos por mes (buenos vs malos)", "Mes", "Número de vuelos")
    else:
        st.info("No se ha encontrado la columna 'month' en los datos.")

    # ======================
    # VUELOS POR DÍA DE LA SEMANA
    # ======================
    st.header("3. Vuelos por día de la semana")

    st.markdown(
        """
Este gráfico agrupa los vuelos por día de la semana.

- Permite ver en qué días hay más operaciones.
- Comparar BUENOS vs MALOS ayuda a detectar si ciertos días son más problemáticos.
"""
    )

    if "day_of_week" in good.columns and "day_of_week" in bad.columns:
        gd = good.groupby("day_of_week")["_id"].count().rename("good")
        bd = bad.groupby("day_of_week")["_id"].count().rename("bad")
        df_d = pd.concat([gd, bd], axis=1).fillna(0).sort_index()
        plot_bar(df_d, "Vuelos por día de la semana (buenos vs malos)", "Día de la semana (1=Lunes, 7=Domingo)", "Número de vuelos")
    else:
        st.info("No se ha encontrado la columna 'day_of_week' en los datos.")

    # ======================
    # DISTRIBUCIÓN DE RETRASOS
    # ======================
    st.header("4. Distribución de retrasos de llegada")

    st.markdown(
        """
Aquí se muestra la distribución de `arr_delay` para vuelos BUENOS y MALOS.

- En los **vuelos buenos** esperamos retrasos concentrados alrededor de 0 y acotados.
- En los **vuelos malos** vemos colas más largas, con mayores retrasos positivos o adelantos exagerados.
"""
    )

    if "arr_delay" in good.columns:
        plot_hist(good["arr_delay"], "Distribución de retrasos - Vuelos BUENOS", "Retraso de llegada (min)", "Frecuencia")
    if "arr_delay" in bad.columns:
        plot_hist(bad["arr_delay"], "Distribución de retrasos - Vuelos MALOS", "Retraso de llegada (min)", "Frecuencia")

    # ======================
    # AEROLÍNEAS
    # ======================
    st.header("5. Aerolíneas con más vuelos")

    st.markdown(
        """
En este gráfico se muestran las **10 aerolíneas con más vuelos** en la muestra, separando BUENOS y MALOS.

- Sirve para analizar el **rendimiento relativo por compañía**.
- No implica directamente calidad global de la aerolínea (podría tener más vuelos y por tanto más retrasos), pero da contexto.
"""
    )

    if "op_unique_carrier" in good.columns and "op_unique_carrier" in bad.columns:
        good_car = good["op_unique_carrier"].value_counts().head(10)
        bad_car = bad["op_unique_carrier"].value_counts().head(10)
        df_car = pd.concat(
            [good_car.rename("good"), bad_car.rename("bad")],
            axis=1,
        ).fillna(0)
        plot_bar(df_car, "Top 10 aerolíneas (vuelos buenos vs malos)", "Aerolínea", "Número de vuelos")
    else:
        st.info("No se ha encontrado la columna 'op_unique_carrier' en los datos.")

    # ======================
    # RUTAS
    # ======================
    st.header("6. Rutas origen-destino más frecuentes")

    st.markdown(
        """
Se construye una ruta como `ORIGIN-DEST` y se muestran las **10 rutas más frecuentes**, diferenciando BUENOS y MALOS.

- Permite identificar **trayectos críticos** con muchos problemas.
- También es útil para ver qué rutas concentran la mayor parte del tráfico.
"""
    )

    if "origin" in good.columns and "dest" in good.columns and "origin" in bad.columns and "dest" in bad.columns:
        good_routes = (
            good.assign(route=good["origin"] + "-" + good["dest"])
            .groupby("route")["_id"]
            .count()
            .sort_values(ascending=False)
            .head(10)
            .rename("good")
        )
        bad_routes = (
            bad.assign(route=bad["origin"] + "-" + bad["dest"])
            .groupby("route")["_id"]
            .count()
            .sort_values(ascending=False)
            .head(10)
            .rename("bad")
        )
        df_routes = pd.concat([good_routes, bad_routes], axis=1).fillna(0)
        plot_bar(df_routes, "Top 10 rutas (vuelos buenos vs malos)", "Ruta ORIGEN-DESTINO", "Número de vuelos")
    else:
        st.info("No se ha podido construir la ruta ORIGIN-DEST por falta de columnas.")

    # ======================
    # DISTANCIA vs RETRASO
    # ======================
    st.header("7. Relación entre distancia y retraso")

    st.markdown(
        """
Este gráfico explora la relación entre la **distancia del vuelo** y el **retraso de llegada**.

- La nube de puntos de vuelos BUENOS suele estar concentrada cerca de `arr_delay ≈ 0`.
- Para vuelos MALOS es posible observar si los vuelos más largos tienden a acumular más retrasos.
"""
    )

    if "distance" in good.columns and "arr_delay" in good.columns:
        st.subheader("Vuelos BUENOS")
        plot_scatter(
            good["distance"],
            good["arr_delay"],
            "Distancia vs retraso (vuelos BUENOS)",
            "Distancia (millas)",
            "Retraso de llegada (min)",
        )
    if "distance" in bad.columns and "arr_delay" in bad.columns:
        st.subheader("Vuelos MALOS")
        plot_scatter(
            bad["distance"],
            bad["arr_delay"],
            "Distancia vs retraso (vuelos MALOS)",
            "Distancia (millas)",
            "Retraso de llegada (min)",
        )

    st.markdown(
        """
En conjunto, estos gráficos permiten justificar tanto:

- La **lógica de calidad del dato** y la separación BUENO/MALO,
- como el **análisis posterior del comportamiento del sistema** por aeropuertos, aerolíneas, rutas y patrones temporales.
"""
    )


if __name__ == "__main__":
    main()
