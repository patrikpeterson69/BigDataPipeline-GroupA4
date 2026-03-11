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

    df = dd.read_parquet(parquet_files, columns=["PULocationID", "DOLocationID", "base_passenger_fare"])

    # Ta bort rader där viktiga kolumner är tomma (Null)
    logger.info("Rensar bort ogiltig data (Null-värden)...")
    df_clean = df.dropna(subset=["base_passenger_fare", "PULocationID", "DOLocationID"])

    # Operation A: Filtrering (Ta bort orimliga resor)
    logger.info("Filtrerar bort resor med negativt pris eller noll passagerare...")
    df_clean = df_clean[df_clean["base_passenger_fare"] > 0]

    # Ladda ner zonfilen om den saknas
    zone_file = BASE_DIR / "data" / "taxi_zone_lookup.csv"
    if not zone_file.exists():
        logger.info("Laddar ner taxi_zone_lookup.csv...")
        r = requests.get("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")
        zone_file.write_bytes(r.content)

    # Joina med taxizoner (zones är liten → pandas, broadcast via merge)
    logger.info("Joinar med taxizoner...")
    zones = pd.read_csv(str(zone_file))

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
    logger.info("Join klar!")

    # Aggregation: snittpris och antal resor per pickup-borough
    logger.info("Aggregerar data per borough...")
    df_agg = (
        df_joined.groupby("pickup_borough")["base_passenger_fare"]
        .agg(["count", "mean"])
        .rename(columns={"count": "antal_resor", "mean": "snitt_pris"})
        .reset_index()
        .compute()
        .sort_values("antal_resor", ascending=False)
    )
    df_agg["snitt_pris"] = df_agg["snitt_pris"].round(2)
    logger.info("Aggregation klar!")
    print(df_agg.to_string(index=False))

    # Window function: ranka zoner per borough baserat på antal resor
    logger.info("Kör window function - rankar zoner per borough...")
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
    logger.info("Window function klar!")
    print(df_ranked.to_string(index=False))

    # Spara resultat
    processed_dir = DATA_DIR / "processed"
    processed_dir.mkdir(exist_ok=True)
    agg_path = str(processed_dir / "agg_per_borough_dask.parquet")
    ranked_path = str(processed_dir / "ranked_zones_dask.parquet")
    logger.info(f"Sparar aggregation till {agg_path}")
    df_agg.to_parquet(agg_path, index=False)
    logger.info(f"Sparar rankade zoner till {ranked_path}")
    df_ranked.to_parquet(ranked_path, index=False)
    logger.info("Pipeline färdig!")


if __name__ == "__main__":
    process_data()
