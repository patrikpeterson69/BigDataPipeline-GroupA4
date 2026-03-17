import os
import sys
import time
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
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.local.io.read.vectored.enabled", "false") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.driver.extraJavaOptions", "-XX:MaxDirectMemorySize=2g -Djdk.nio.maxCachedBufferSize=0") \
        .config("spark.sql.shuffle.partitions", "200") \
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

    timings = {}
    pipeline_start = time.time()

    t0 = time.time()
    df = spark.read.parquet(*parquet_files).select(
        "base_passenger_fare",
        "PULocationID",
        "DOLocationID"
    )
    timings["Inläsning"] = time.time() - t0
    logger.info(f"[TIMING] Inläsning: {timings['Inläsning']:.2f}s")

    # Ta bort rader där viktiga kolumner är tomma (Null)
    logger.info("Rensar bort ogiltig data (Null-värden)...")
    t0 = time.time()
    df_clean = df.dropna(subset=["base_passenger_fare", "PULocationID", "DOLocationID"])
    df_clean = df_clean.filter(col("base_passenger_fare") > 0)
    timings["Filtrering + dropna"] = time.time() - t0
    logger.info(f"[TIMING] Filtrering + dropna: {timings['Filtrering + dropna']:.2f}s")

    # Ladda ner zonfilen om den saknas
    zone_file = BASE_DIR / "data" / "taxi_zone_lookup.csv"
    if not zone_file.exists():
        logger.info("Laddar ner taxi_zone_lookup.csv...")
        r = requests.get("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")
        zone_file.write_bytes(r.content)

    # Joina med taxizoner
    logger.info("Joinar med taxizoner...")
    zones = spark.read.csv(str(zone_file), header=True, inferSchema=True)

    # Droppar nullvärnden i zonesfilen
    zones_clean = zones.select(
        col("LocationID").alias("LocationID"),
        col("Zone").alias("Zone"),
        col("Borough").alias("Borough")
    ).dropna(subset=["LocationID", "Zone", "Borough"]).dropDuplicates(["LocationID"])

    t0 = time.time()
    df_joined = df_clean.join(
        broadcast(zones_clean).select(col("LocationID").alias("PULocationID"),
                     col("Zone").alias("pickup_zone"),
                     col("Borough").alias("pickup_borough")),
        on="PULocationID", how="left"
    )
    df_joined = df_joined.join(
        broadcast(zones_clean).select(col("LocationID").alias("DOLocationID"),
                     col("Zone").alias("dropoff_zone"),
                     col("Borough").alias("dropoff_borough")),
        on="DOLocationID", how="left"
    )
    timings["Join med taxizoner"] = time.time() - t0
    logger.info(f"[TIMING] Join med taxizoner: {timings['Join med taxizoner']:.2f}s")

    # Aggregation: snittpris och antal resor per pickup-borough
    logger.info("Aggregerar data per borough...")
    t0 = time.time()
    df_agg = df_joined.groupBy("pickup_borough").agg(
        count("*").alias("antal_resor"),
        round(avg("base_passenger_fare"), 2).alias("snitt_pris")
    ).orderBy(col("antal_resor").desc())
    df_agg.show()
    timings["Aggregation per borough"] = time.time() - t0
    logger.info(f"[TIMING] Aggregation per borough: {timings['Aggregation per borough']:.2f}s")

    # Window function: ranka zoner per borough baserat på antal resor
    logger.info("Kör window function - rankar zoner per borough...")
    t0 = time.time()
    zone_counts = df_joined.groupBy("pickup_borough", "pickup_zone").agg(
        count("*").alias("antal_resor")
    )
    window_spec = Window.partitionBy("pickup_borough").orderBy(col("antal_resor").desc())
    df_ranked = zone_counts.withColumn("rank", rank().over(window_spec))
    df_ranked.filter(col("rank") <= 3).orderBy("pickup_borough", "rank").show(truncate=False)
    timings["Window function"] = time.time() - t0
    logger.info(f"[TIMING] Window function: {timings['Window function']:.2f}s")

    # Spara aggregationsresultaten via pandas (undviker Hadoop write-committer på Windows)
    processed_dir = DATA_DIR / "processed"
    processed_dir.mkdir(exist_ok=True)
    agg_path = str(processed_dir / "agg_per_borough.parquet")
    ranked_path = str(processed_dir / "ranked_zones.parquet")
    t0 = time.time()
    df_agg.toPandas().to_parquet(agg_path, index=False)
    df_ranked.filter(col("rank") <= 3).toPandas().to_parquet(ranked_path, index=False)
    timings["Spara till parquet"] = time.time() - t0
    logger.info(f"[TIMING] Spara till parquet: {timings['Spara till parquet']:.2f}s")

    timings["Total pipeline"] = time.time() - pipeline_start
    logger.info(f"[TIMING] Total pipeline: {timings['Total pipeline']:.2f}s")

    df_clean.unpersist()
    return timings
if __name__ == "__main__":
    spark = create_spark_session()
    process_data(spark)
    # Stäng alltid Spark snyggt
    spark.stop()