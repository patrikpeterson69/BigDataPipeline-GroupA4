from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round, rank
from pyspark.sql.window import Window
from utils import get_logger
import requests
from pathlib import Path

logger = get_logger("Transformation")

def create_spark_session():
    logger.info("Startar Apache Spark-session...")
    return SparkSession.builder \
        .appName("NYCTaxi_Pipeline") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def process_data(spark, input_path="data/*.parquet", output_path="data/processed/"):
    logger.info(f"Läser in all Parquet-data från {input_path}")
    
    # Läs in alla filer som matchar mönstret samtidigt
    df = spark.read.parquet(input_path)
    

    initial_count = df.count()
    logger.info(f"Totalt antal rader inlästa: {initial_count}")
    
    # Ta bort rader där viktiga kolumner är tomma (Null)
    logger.info("Rensar bort ogiltig data (Null-värden)...")
    df_clean = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "total_amount"])

    # Operation A: Filtrering (Ta bort orimliga resor)
    logger.info("Filtrerar bort resor med negativt pris eller noll passagerare...")
    df_clean = df_clean.filter(
        (col("total_amount") > 0) & 
        (col("passenger_count") > 0)
    )
    
    # Beräkna hur mycket vi rensade bort
    final_count = df_clean.count()
    logger.info(f"Rader kvar efter tvätt: {final_count} (Tog bort {initial_count - final_count} rader)")
    
    # Ladda ner zonfilen om den saknas
    zone_file = Path("data/taxi_zone_lookup.csv")
    ### if not zone_file.exists():
       # logger.info("Laddar ner taxi_zone_lookup.csv...")
       # r = requests.get("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")
        #zone_file.write_bytes(r.content)

    # Joina med taxizoner
    logger.info("Joindar med taxizoner...")
    zones = spark.read.csv(str(zone_file), header=True, inferSchema=True)

    df_joined = df_clean.join(
        zones.select(col("LocationID").alias("PULocationID"),
                     col("Zone").alias("pickup_zone"),
                     col("Borough").alias("pickup_borough")),
        on="PULocationID", how="left"
    )
    df_joined = df_joined.join(
        zones.select(col("LocationID").alias("DOLocationID"),
                     col("Zone").alias("dropoff_zone"),
                     col("Borough").alias("dropoff_borough")),
        on="DOLocationID", how="left"
    )
    logger.info("Join klar!")
    
    # Aggregation: snittpris och antal resor per pickup-borough
    logger.info("Aggregerar data per borough...")
    df_agg = df_joined.groupBy("pickup_borough").agg(
        count("*").alias("antal_resor"),
        round(avg("base_passenger_fare"), 2).alias("snitt_pris")
    ).orderBy(col("antal_resor").desc())
    logger.info("Aggregation klar!")
    df_agg.show()
    
    # Window function: ranka zoner per borough baserat på antal resor
    logger.info("Kör window function - rankar zoner per borough...")
    zone_counts = df_joined.groupBy("pickup_borough", "pickup_zone").agg(
        count("*").alias("antal_resor")
    )
    window_spec = Window.partitionBy("pickup_borough").orderBy(col("antal_resor").desc())
    df_ranked = zone_counts.withColumn("rank", rank().over(window_spec))
    logger.info("Window function klar!")
    df_ranked.filter(col("rank") <= 3).orderBy("pickup_borough", "rank").show(truncate=False)


    logger.info(f"Sparar bearbetad data till {output_path}")

    df_clean.write.mode("overwrite").parquet(output_path)
    logger.info("Pipeline färdig!")

if __name__ == "__main__":
    spark = create_spark_session()
    process_data(spark)
    # Stäng alltid Spark snyggt
    spark.stop()