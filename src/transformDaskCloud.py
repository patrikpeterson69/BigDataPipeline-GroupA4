import dask.dataframe as dd
import pandas as pd
import requests
from pathlib import Path
from utils import get_logger
import config

logger = get_logger("TransformationDaskCloud")

# På molnet (t.ex. Azure ML) används adlfs för att läsa från ADLS Gen2:
#   pip install adlfs
#   storage_options = {"account_name": AZURE_STORAGE_ACCOUNT,
#                      "tenant_id": ..., "client_id": ..., "client_secret": ...}
#   dd.read_parquet("abfs://container@account.dfs.core.windows.net/raw/*.parquet",
#                   storage_options=storage_options)


def process_data():
    # --- Läs parquet-filer ---
    if config.ENV == "databricks":
        # Dask körs inte på Databricks — använd transformSparkCloud.py istället.
        # Den här koden visar hur det skulle se ut på Azure ML / en vanlig VM i molnet.
        import os
        storage_options = {
            "account_name": os.getenv("AZURE_STORAGE_ACCOUNT"),
            "tenant_id":    os.getenv("AZURE_TENANT_ID"),
            "client_id":    os.getenv("AZURE_CLIENT_ID"),
            "client_secret": os.getenv("AZURE_CLIENT_SECRET"),
        }
        parquet_path = f"abfs://nyctaxi@{storage_options['account_name']}.dfs.core.windows.net/raw/*.parquet"
        logger.info(f"Läser från Azure Data Lake: {parquet_path}")
        df = dd.read_parquet(parquet_path, storage_options=storage_options)
    else:
        base_dir = Path(__file__).parent.parent
        parquet_files = [str(f) for f in (base_dir / "data").glob("fhvhv_tripdata_*.parquet")]
        logger.info(f"Hittade {len(parquet_files)} parquet-filer lokalt")
        df = dd.read_parquet(
            parquet_files,
            columns=["PULocationID", "DOLocationID", "base_passenger_fare"]
        )

    # Ta bort rader där viktiga kolumner är tomma (Null)
    logger.info("Rensar bort ogiltig data (Null-värden)...")
    df_clean = df.dropna(subset=["base_passenger_fare", "PULocationID", "DOLocationID"])

    # Operation A: Filtrering
    logger.info("Filtrerar bort resor med negativt pris...")
    df_clean = df_clean[df_clean["base_passenger_fare"] > 0]

    # Ladda ner / läs zonfilen
    if config.ENV == "databricks":
        import adlfs
        fs = adlfs.AzureBlobFileSystem(**storage_options)
        with fs.open(config.ZONE_FILE_PATH) as f:
            zones = pd.read_csv(f)
    else:
        zone_file = Path(config.ZONE_FILE_PATH)
        if not zone_file.exists():
            logger.info("Laddar ner taxi_zone_lookup.csv...")
            r = requests.get("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")
            zone_file.write_bytes(r.content)
        zones = pd.read_csv(str(zone_file))

    # Joina med taxizoner (zones är liten → pandas-merge, fungerar som broadcast)
    logger.info("Joinar med taxizoner...")
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

    # Window function: ranka zoner per borough
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
    processed_dir = Path(config.PROCESSED_PATH)
    processed_dir.mkdir(exist_ok=True)
    agg_path    = str(processed_dir / "agg_per_borough_dask.parquet")
    ranked_path = str(processed_dir / "ranked_zones_dask.parquet")
    logger.info(f"Sparar aggregation till {agg_path}")
    df_agg.to_parquet(agg_path, index=False)
    df_ranked.to_parquet(ranked_path, index=False)
    logger.info("Pipeline färdig!")


if __name__ == "__main__":
    process_data()
