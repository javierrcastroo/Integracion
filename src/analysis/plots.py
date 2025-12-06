import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt

MONGO_URI = "mongodb://javier:1234@localhost:27017/"
DB_NAME = "flights_db"

def load_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    good = pd.DataFrame(list(db["good_flights_clean"].find()))
    bad = pd.DataFrame(list(db["bad_flights_clean"].find()))

    return good, bad

def plot_top_origins(good, bad, top_n=10):
    good_counts = good["origin"].value_counts().head(top_n)
    bad_counts = bad["origin"].value_counts().head(top_n)

    # Unimos en un solo DataFrame para compararlo
    df = pd.DataFrame({
        "good": good_counts,
        "bad": bad_counts
    }).fillna(0)

    ax = df.plot(kind="bar")
    ax.set_title(f"Top {top_n} origins - good vs bad flights")
    ax.set_xlabel("Origin airport")
    ax.set_ylabel("Number of flights")
    plt.tight_layout()
    plt.show()

def plot_delay_histograms(good, bad):
    # Histogramas de retrasos buenos y malos
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))

    axes[0].hist(good["arr_delay"].dropna(), bins=20)
    axes[0].set_title("Good flights - arr_delay")
    axes[0].set_xlabel("Delay (min)")
    axes[0].set_ylabel("Count")

    axes[1].hist(bad["arr_delay"].dropna(), bins=20)
    axes[1].set_title("Bad flights - arr_delay")
    axes[1].set_xlabel("Delay (min)")
    axes[1].set_ylabel("Count")

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    good_df, bad_df = load_data()
    plot_top_origins(good_df, bad_df)
    plot_delay_histograms(good_df, bad_df)
