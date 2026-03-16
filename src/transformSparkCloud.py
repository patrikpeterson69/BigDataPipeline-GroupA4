import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round, rank, broadcast
from pyspark.sql.window import Window
import requests
from utils import get_logger
import config

logger = get_logger("TransformationSparkCloud")


def get_spark_session():
    """Returnerar aktiv Spark-session (Databricks) eller skapar en lokal."""
    active = SparkSession.getActiveSession()
    if active:
        logger.info("Använder befintlig Spark-session (Databricks)")
        # Konfigurera ADLS-åtkomst med access key
        if config.ENV == "databricks" and config.ACCESS_KEY:
            active.conf.set(
                f"fs.azure.account.key.{config.STORAGE_ACCOUNT}.dfs.core.windows.net",
                config.ACCESS_KEY
            )
        return active

    logger.info("Startar lokal Apache Spark-session...")
    BASE_DIR = Path(__file__).parent.parent
    if sys.platform == "win32":
        os.environ.setdefault("HADOOP_HOME", str(BASE_DIR))
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    builder = SparkSession.builder \
        .appName("NYCTaxi_Pipeline_Cloud") \
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.local.io.read.vectored.enabled", "false") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \

    if config.ENV == "databricks" and config.ACCESS_KEY:
        # Lägg till Azure ADLS-connector och konfigurera åtkomst
        builder = builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.4.0") \
            .config(f"fs.azure.account.key.{config.STORAGE_ACCOUNT}.blob.core.windows.net",
                    config.ACCESS_KEY)

    return builder \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.driver.extraJavaOptions", "-XX:MaxDirectMemorySize=2g -Djdk.nio.maxCachedBufferSize=0") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "4") \
        .getOrCreate()


def process_data(spark):
    # --- Läs parquet-filer ---
    if config.ENV == "databricks":
        parquet_path = f"{config.RAW_PATH}/fhvhv_tripdata_*.parquet"
        logger.info(f"Läser från Azure Data Lake: {parquet_path}")
        df = spark.read.parquet(parquet_path)
    else:
        base_dir = Path(__file__).parent.parent
        parquet_files = [str(f) for f in (base_dir / "data").glob("fhvhv_tripdata_*.parquet")]
        logger.info(f"Hittade {len(parquet_files)} parquet-filer lokalt")
        df = spark.read.parquet(*parquet_files)

    # Ta bort rader där viktiga kolumner är tomma (Null)
    logger.info("Rensar bort ogiltig data (Null-värden)...")
    df_clean = df.dropna(subset=["base_passenger_fare", "PULocationID", "DOLocationID"])

    # Operation A: Filtrering
    logger.info("Filtrerar bort resor med negativt pris...")
    df_clean = df_clean.filter(col("base_passenger_fare") > 0)

    # Ladda ner zonfilen om den saknas (lokalt) eller läs från molnet
    if config.ENV == "databricks":
        zones = spark.read.csv(config.ZONE_FILE_PATH, header=True, inferSchema=True)
    else:
        zone_file = Path(config.ZONE_FILE_PATH)
        if not zone_file.exists():
            logger.info("Laddar ner taxi_zone_lookup.csv...")
            r = requests.get("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")
            zone_file.write_bytes(r.content)
        zones = spark.read.csv(str(zone_file), header=True, inferSchema=True)

    # Joina med taxizoner
    logger.info("Joinar med taxizoner...")
    df_joined = df_clean.join(
        broadcast(zones).select(
            col("LocationID").alias("PULocationID"),
            col("Zone").alias("pickup_zone"),
            col("Borough").alias("pickup_borough")
        ),
        on="PULocationID", how="left"
    )
    df_joined = df_joined.join(
        broadcast(zones).select(
            col("LocationID").alias("DOLocationID"),
            col("Zone").alias("dropoff_zone"),
            col("Borough").alias("dropoff_borough")
        ),
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

    # Window function: ranka zoner per borough
    logger.info("Kör window function - rankar zoner per borough...")
    zone_counts = df_joined.groupBy("pickup_borough", "pickup_zone").agg(
        count("*").alias("antal_resor")
    )
    window_spec = Window.partitionBy("pickup_borough").orderBy(col("antal_resor").desc())
    df_ranked = zone_counts.withColumn("rank", rank().over(window_spec))
    logger.info("Window function klar!")
    df_ranked.filter(col("rank") <= 3).orderBy("pickup_borough", "rank").show(truncate=False)

    # Spara resultat
    agg_path    = f"{config.PROCESSED_PATH}/agg_per_borough.parquet"
    ranked_path = f"{config.PROCESSED_PATH}/ranked_zones.parquet"
    logger.info(f"Sparar aggregation till {agg_path}")

    if config.ENV == "databricks":
        # På Databricks skriver Spark direkt till ADLS — inga winutils-problem
        df_agg.write.mode("overwrite").parquet(agg_path)
        df_ranked.filter(col("rank") <= 3).write.mode("overwrite").parquet(ranked_path)
    else:
        Path(config.PROCESSED_PATH).mkdir(exist_ok=True)
        df_agg.toPandas().to_parquet(agg_path, index=False)
        df_ranked.filter(col("rank") <= 3).toPandas().to_parquet(ranked_path, index=False)

    logger.info("Pipeline färdig!")
    df_clean.unpersist()


if __name__ == "__main__":
    spark = get_spark_session()
    process_data(spark)
    if config.ENV != "databricks":
        spark.stop()
