import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round, rank, broadcast
from pyspark.sql.window import Window
from utils import get_logger
import requests
from pathlib import Path

logger = get_logger("Transformation")

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

def create_spark_session():
    logger.info("Startar Apache Spark-session...")
    # Fixa Windows-kompatibilitet
    if sys.platform == "win32":
        os.environ.setdefault("HADOOP_HOME", str(BASE_DIR))
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return SparkSession.builder \
        .appName("NYCTaxi_Pipeline") \
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.local.io.read.vectored.enabled", "false") \
        .config("spark.driver.extraJavaOptions", "-XX:MaxDirectMemorySize=8g -Djdk.nio.maxCachedBufferSize=0") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "4") \
        .getOrCreate()

def process_data(spark, input_path=None, output_path=None):
    if input_path is None:
        input_path = str(DATA_DIR / "*.parquet")
    if output_path is None:
        output_path = str(DATA_DIR / "processed")
    # Lista filer med Python istället för glob (Windows-kompatibelt)
    parquet_files = [str(f) for f in DATA_DIR.glob("fhvhv_tripdata_*.parquet")]
    logger.info(f"Hittade {len(parquet_files)} parquet-filer i {DATA_DIR}")

    df = spark.read.parquet(*parquet_files)
    

    initial_count = df.count()
    logger.info(f"Totalt antal rader inlästa: {initial_count}")
    
    # Ta bort rader där viktiga kolumner är tomma (Null)
    logger.info("Rensar bort ogiltig data (Null-värden)...")
    df_clean = df.dropna(subset=["pickup_datetime", "dropoff_datetime", "base_passenger_fare"])

    # Operation A: Filtrering (Ta bort orimliga resor)
    logger.info("Filtrerar bort resor med negativt pris eller noll passagerare...")
    df_clean = df_clean.filter(
        col("base_passenger_fare") > 0
    )

    df_clean.cache()
    
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
        broadcast(zones).select(col("LocationID").alias("PULocationID"),
                     col("Zone").alias("pickup_zone"),
                     col("Borough").alias("pickup_borough")),
        on="PULocationID", how="left"
    )
    df_joined = df_joined.join(
        broadcast(zones).select(col("LocationID").alias("DOLocationID"),
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


    # Spara aggregationsresultaten via pandas (undviker Hadoop write-committer på Windows)
    processed_dir = DATA_DIR / "processed"
    processed_dir.mkdir(exist_ok=True)
    agg_path = str(processed_dir / "agg_per_borough.parquet")
    ranked_path = str(processed_dir / "ranked_zones.parquet")
    logger.info(f"Sparar aggregation till {agg_path}")
    df_agg.write.mode("overwrite").parquet(agg_path)
    logger.info(f"Sparar rankade zoner till {ranked_path}")
    df_ranked.filter(col("rank") <= 3).write.mode("overwrite").parquet(ranked_path)
    logger.info("Pipeline färdig!")

    df_clean.unpersist()
if __name__ == "__main__":
    spark = create_spark_session()
    process_data(spark)
    # Stäng alltid Spark snyggt
    spark.stop()