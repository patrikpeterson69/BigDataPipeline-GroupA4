import dask.dataframe as dd
from utils import get_logger
from pathlib import Path

# Ladda ner zonfilen om den saknas
zone_file = Path("data/taxi_zone_lookup.csv")

logger = get_logger("Transformation_Dask")

def process_data(input_path="data/*.parquet", output_path="data/processed_dask/"):
    logger.info(f"Läser in all Parquet-data från {input_path}")

    # Läs in alla filer som matchar mönstret samtidigt
    df = dd.read_parquet(input_path)

    initial_count = len(df)
    logger.info(f"Totalt antal rader inlästa: {initial_count}")

    # Ta bort rader där viktiga kolumner är tomma (Null)
    logger.info("Rensar bort ogiltig data (Null-värden)...")
    df_clean = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "total_amount"])

    # Operation A: Filtrering (Ta bort orimliga resor)
    logger.info("Filtrerar bort resor med negativt pris eller noll passagerare...")
    df_clean = df_clean[(df_clean["total_amount"] > 0) & (df_clean["passenger_count"] > 0)]

    # Beräkna hur mycket vi rensade bort
    final_count = len(df_clean)
    logger.info(f"Rader kvar efter tvätt: {final_count} (Tog bort {initial_count - final_count} rader)")

    # Joina med taxizoner
    logger.info("Join med taxizoner...")
    zones = dd.read_csv(str(zone_file))

    df_joined = df_clean.merge(
        zones[["LocationID", "Zone", "Borough"]].rename(columns={"LocationID": "PULocationID", "Zone": "pickup_zone", "Borough": "pickup_borough"}),
        on="PULocationID", how="left"
    )
    df_joined = df_joined.merge(
        zones[["LocationID", "Zone", "Borough"]].rename(columns={"LocationID": "DOLocationID", "Zone": "dropoff_zone", "Borough": "dropoff_borough"}),
        on="DOLocationID", how="left"
    )
    logger.info("Join klar")

    # TODO: Lägg till Aggregation (t.ex. snittpris per zon)
    # TODO: Lägg till Window function (för VG-krav)

    logger.info(f"Sparar bearbetad data till {output_path}")

    df_joined.to_parquet(output_path)
    logger.info("Pipeline färdig")

if __name__ == "__main__":
    process_data()