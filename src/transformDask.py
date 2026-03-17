import time
import dask.dataframe as dd
import pandas as pd
import requests
from pathlib import Path
from utils import get_logger

logger = get_logger("TransformationDask")

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"


def process_data(input_path=None, output_path=None):
    if output_path is None:
        output_path = str(DATA_DIR / "processed")

    parquet_files = [str(f) for f in DATA_DIR.glob("fhvhv_tripdata_*.parquet")]
    logger.info(f"Hittade {len(parquet_files)} parquet-filer i {DATA_DIR}")

    timings = {}
    pipeline_start = time.time()

    t0 = time.time()
    df = dd.read_parquet(parquet_files, columns=["PULocationID", "DOLocationID", "base_passenger_fare"])
    timings["Inläsning"] = time.time() - t0
    logger.info(f"[TIMING] Inläsning: {timings['Inläsning']:.2f}s")

    # Ta bort rader där viktiga kolumner är tomma (Null)
    logger.info("Rensar bort ogiltig data (Null-värden)...")
    t0 = time.time()
    df_clean = df.dropna(subset=["base_passenger_fare", "PULocationID", "DOLocationID"])
    df_clean = df_clean[df_clean["base_passenger_fare"] > 0]
    timings["Filtrering + dropna"] = time.time() - t0
    logger.info(f"[TIMING] Filtrering + dropna: {timings['Filtrering + dropna']:.2f}s")

    # Ladda ner zonfilen om den saknas
    zone_file = BASE_DIR / "data" / "taxi_zone_lookup.csv"
    if not zone_file.exists():
        logger.info("Laddar ner taxi_zone_lookup.csv...")
        r = requests.get("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")
        zone_file.write_bytes(r.content)

    # Joina med taxizoner (zones är liten → pandas, broadcast via merge)
    logger.info("Joinar med taxizoner...")
    zones = pd.read_csv(str(zone_file))

    t0 = time.time()
    df_joined = df_clean.merge(
        zones[["LocationID", "Zone", "Borough"]].rename(columns={
            "LocationID": "PULocationID",
            "Zone": "pickup_zone",
            "Borough": "pickup_borough"
        }),
        on="PULocationID", how="left"
    )
    df_joined = df_joined.merge(
        zones[["LocationID", "Zone", "Borough"]].rename(columns={
            "LocationID": "DOLocationID",
            "Zone": "dropoff_zone",
            "Borough": "dropoff_borough"
        }),
        on="DOLocationID", how="left"
    )
    timings["Join med taxizoner"] = time.time() - t0
    logger.info(f"[TIMING] Join med taxizoner: {timings['Join med taxizoner']:.2f}s")

    # Aggregation: snittpris och antal resor per pickup-borough
    logger.info("Aggregerar data per borough...")
    t0 = time.time()
    df_agg = (
        df_joined.groupby("pickup_borough")["base_passenger_fare"]
        .agg(["count", "mean"])
        .rename(columns={"count": "antal_resor", "mean": "snitt_pris"})
        .reset_index()
        .compute()
        .sort_values("antal_resor", ascending=False)
    )
    df_agg["snitt_pris"] = df_agg["snitt_pris"].round(2)
    timings["Aggregation per borough"] = time.time() - t0
    logger.info(f"[TIMING] Aggregation per borough: {timings['Aggregation per borough']:.2f}s")
    print(df_agg.to_string(index=False))

    # Window function: ranka zoner per borough baserat på antal resor
    logger.info("Kör window function - rankar zoner per borough...")
    t0 = time.time()
    zone_counts = (
        df_joined.groupby(["pickup_borough", "pickup_zone"])
        .size()
        .reset_index()
        .rename(columns={0: "antal_resor"})
        .compute()
    )
    zone_counts["rank"] = (
        zone_counts.groupby("pickup_borough")["antal_resor"]
        .rank(method="min", ascending=False)
        .astype(int)
    )
    df_ranked = zone_counts[zone_counts["rank"] <= 3].sort_values(["pickup_borough", "rank"])
    timings["Window function"] = time.time() - t0
    logger.info(f"[TIMING] Window function: {timings['Window function']:.2f}s")
    print(df_ranked.to_string(index=False))

    # Spara resultat
    processed_dir = DATA_DIR / "processed"
    processed_dir.mkdir(exist_ok=True)
    agg_path = str(processed_dir / "agg_per_borough_dask.parquet")
    ranked_path = str(processed_dir / "ranked_zones_dask.parquet")
    t0 = time.time()
    df_agg.to_parquet(agg_path, index=False)
    df_ranked.to_parquet(ranked_path, index=False)
    timings["Spara till parquet"] = time.time() - t0
    logger.info(f"[TIMING] Spara till parquet: {timings['Spara till parquet']:.2f}s")

    timings["Total pipeline"] = time.time() - pipeline_start
    logger.info(f"[TIMING] Total pipeline: {timings['Total pipeline']:.2f}s")
    logger.info("Pipeline färdig!")
    return timings


if __name__ == "__main__":
    from dask.distributed import Client
    import webbrowser

    client = Client(n_workers=2, threads_per_worker=4, processes=False, dashboard_address="localhost:8787")
    logger.info(f"Dask dashboard: http://localhost:8787/status")
    webbrowser.open("http://localhost:8787/status")

    process_data()

    client.close()
