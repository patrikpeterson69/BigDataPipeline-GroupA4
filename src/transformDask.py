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

    zones = pd.read_csv(str(zone_file))
    zones_pu = zones[["LocationID", "Zone", "Borough"]].rename(columns={
        "LocationID": "PULocationID",
        "Zone": "pickup_zone",
        "Borough": "pickup_borough"
    })

    # Aggregation: aggregera per PULocationID först (numeriska ID:n, litet resultat)
    # Joina sedan zones på det lilla aggregerade resultatet (~265 rader) istället för på hela datasetet
    logger.info("Aggregerar data per borough...")
    t0 = time.time()
    agg_by_loc = (
        df_clean.groupby("PULocationID")["base_passenger_fare"]
        .agg(["count", "sum"])
        .compute()
        .reset_index()
    )
    agg_by_loc = agg_by_loc.merge(zones_pu[["PULocationID", "pickup_borough"]], on="PULocationID", how="left")
    df_agg = (
        agg_by_loc.groupby("pickup_borough")
        .agg(antal_resor=("count", "sum"), total_fare=("sum", "sum"))
        .reset_index()
    )
    df_agg["snitt_pris"] = (df_agg["total_fare"] / df_agg["antal_resor"]).round(2)
    df_agg = df_agg.drop(columns="total_fare").sort_values("antal_resor", ascending=False)
    timings["Aggregation per borough"] = time.time() - t0
    logger.info(f"[TIMING] Aggregation per borough: {timings['Aggregation per borough']:.2f}s")
    print(df_agg.to_string(index=False))

    # Window function: aggregera per (PULocationID) → joina zones → ranka per borough
    logger.info("Kör window function - rankar zoner per borough...")
    t0 = time.time()
    zone_counts_by_loc = (
        df_clean.groupby("PULocationID")
        .size()
        .compute()
        .reset_index()
        .rename(columns={0: "antal_resor"})
    )
    zone_counts = zone_counts_by_loc.merge(zones_pu, on="PULocationID", how="left").dropna(subset=["pickup_borough", "pickup_zone"])
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

    client = Client(n_workers=8, threads_per_worker=4, processes=False, memory_limit=0, dashboard_address="localhost:8787")
    logger.info(f"Dask dashboard: http://localhost:8787/status")

    process_data()

    client.close()
