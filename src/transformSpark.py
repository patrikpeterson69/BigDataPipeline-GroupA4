import os
import sys
import time
from pathlib import Path

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    rank,
    broadcast,
    sum as spark_sum,
    round,
)
from pyspark.sql.window import Window

from utils import get_logger

logger = get_logger("Transformation")

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_INPUT_GLOB = "fhvhv_tripdata_*.parquet"
ZONE_FILE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def create_spark_session():
    logger.info("Startar Apache Spark-session...")

    if sys.platform == "win32":
        os.environ.setdefault("HADOOP_HOME", str(BASE_DIR))
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    return (
        SparkSession.builder
        .appName("NYCTaxi_Pipeline")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.local.io.read.vectored.enabled", "false")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-XX:MaxDirectMemorySize=2g -Djdk.nio.maxCachedBufferSize=0",
        )
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )


def get_parquet_files(input_dir: Path, pattern: str = DEFAULT_INPUT_GLOB):
    parquet_files = [str(f) for f in input_dir.glob(pattern)]

    if not parquet_files:
        raise FileNotFoundError(
            f"Inga parquet-filer hittades i {input_dir} med mönstret {pattern}"
        )

    logger.info(f"Hittade {len(parquet_files)} parquet-filer i {input_dir}")
    return parquet_files


def load_clean_and_aggregate_trip_data(spark, parquet_files):
    """
    Steg 1:
    - Läs in endast nödvändiga kolumner
    - Rensa bort null-värden
    - Filtrera bort ogiltiga priser
    - Tidig aggregering för att minska datamängden
    """
    logger.info("Läser in, rensar och aggregerar resedata...")

    return (
        spark.read.parquet(*parquet_files)
        .select("base_passenger_fare", "PULocationID", "DOLocationID")
        .dropna(subset=["base_passenger_fare", "PULocationID", "DOLocationID"])
        .filter(col("base_passenger_fare") > 0)
        .groupBy("PULocationID", "DOLocationID")
        .agg(
            count("*").alias("antal_resor"),
            spark_sum("base_passenger_fare").alias("sum_fare"),
        )
    )


def ensure_zone_file(zone_file: Path):
    if not zone_file.exists():
        logger.info("Laddar ner taxi_zone_lookup.csv...")
        response = requests.get(ZONE_FILE_URL, timeout=30)
        response.raise_for_status()
        zone_file.write_bytes(response.content)


def load_and_clean_zones(spark, zone_file: Path):
    logger.info("Läser in och rensar taxizoner...")

    zones = spark.read.csv(str(zone_file), header=True, inferSchema=True)

    return (
        zones.select("LocationID", "Zone", "Borough")
        .dropna(subset=["LocationID", "Zone", "Borough"])
        .dropDuplicates(["LocationID"])
    )


def join_with_zones(df_early_agg, zones_clean):
    logger.info("Joinar aggregerad resedata med taxizoner...")

    pickup_zones = broadcast(
        zones_clean.select(
            col("LocationID").alias("PULocationID"),
            col("Zone").alias("pickup_zone"),
            col("Borough").alias("pickup_borough"),
        )
    )

    dropoff_zones = broadcast(
        zones_clean.select(
            col("LocationID").alias("DOLocationID"),
            col("Zone").alias("dropoff_zone"),
            col("Borough").alias("dropoff_borough"),
        )
    )

    return (
        df_early_agg.join(pickup_zones, on="PULocationID", how="left")
        .join(dropoff_zones, on="DOLocationID", how="left")
    )


def aggregate_by_borough(df_joined):
    logger.info("Aggregerar data per pickup_borough...")

    return (
        df_joined.groupBy("pickup_borough")
        .agg(
            spark_sum("antal_resor").alias("antal_resor"),
            round(
                spark_sum("sum_fare") / spark_sum("antal_resor"),
                2
            ).alias("snitt_pris"),
        )
        .orderBy(col("antal_resor").desc())
    )


def rank_top_zones_per_borough(df_joined, top_n=3):
    logger.info("Rankar toppzoner per borough...")

    zone_counts = (
        df_joined.groupBy("pickup_borough", "pickup_zone")
        .agg(spark_sum("antal_resor").alias("antal_resor"))
    )

    window_spec = Window.partitionBy("pickup_borough").orderBy(
        col("antal_resor").desc()
    )

    df_ranked = zone_counts.withColumn("rank", rank().over(window_spec))

    return (
        df_ranked.filter(col("rank") <= top_n)
        .orderBy("pickup_borough", "rank")
    )


def save_results(df_agg, df_top_zones, output_dir: Path):
    logger.info("Sparar resultat...")
    output_dir.mkdir(parents=True, exist_ok=True)

    agg_path = output_dir / "agg_per_borough.parquet"
    ranked_path = output_dir / "ranked_zones.parquet"

    df_agg.toPandas().to_parquet(agg_path, index=False)
    df_top_zones.toPandas().to_parquet(ranked_path, index=False)

    logger.info(f"Spark resultat sparat till: {agg_path}")
    logger.info(f"Spark resultat sparat till: {ranked_path}")


def process_data(spark, input_path=None, output_path=None):
    input_dir = Path(input_path) if input_path else DATA_DIR
    output_dir = Path(output_path) if output_path else (DATA_DIR / "processed")
    zone_file = DATA_DIR / "taxi_zone_lookup.csv"

    timings = {}
    pipeline_start = time.time()

    t0 = time.time()
    parquet_files = get_parquet_files(input_dir)
    df_early_agg = load_clean_and_aggregate_trip_data(spark, parquet_files)
    timings["Inläsning + rensning + tidig aggregering"] = time.time() - t0
    logger.info(
        f"[TIMING] Inläsning + rensning + tidig aggregering: "
        f"{timings['Inläsning + rensning + tidig aggregering']:.2f}s"
    )

    t0 = time.time()
    ensure_zone_file(zone_file)
    zones_clean = load_and_clean_zones(spark, zone_file)
    timings["Inläsning + rensning av zondata"] = time.time() - t0
    logger.info(
        f"[TIMING] Inläsning + rensning av zondata: "
        f"{timings['Inläsning + rensning av zondata']:.2f}s"
    )

    t0 = time.time()
    df_joined = join_with_zones(df_early_agg, zones_clean)
    timings["Join med taxizoner"] = time.time() - t0
    logger.info(f"[TIMING] Join med taxizoner: {timings['Join med taxizoner']:.2f}s")

    t0 = time.time()
    df_agg = aggregate_by_borough(df_joined)
    timings["Aggregation per borough"] = time.time() - t0
    logger.info(
        f"[TIMING] Aggregation per borough: {timings['Aggregation per borough']:.2f}s"
    )
    df_agg.show(truncate=False)

    t0 = time.time()
    df_top_zones = rank_top_zones_per_borough(df_joined, top_n=3)
    timings["Window function"] = time.time() - t0
    logger.info(f"[TIMING] Window function: {timings['Window function']:.2f}s")
    df_top_zones.show(truncate=False)

    t0 = time.time()
    save_results(df_agg, df_top_zones, output_dir)
    timings["Spara till parquet"] = time.time() - t0
    logger.info(f"[TIMING] Spara till parquet: {timings['Spara till parquet']:.2f}s")

    timings["Total pipeline"] = time.time() - pipeline_start
    logger.info(f"[TIMING] Total pipeline: {timings['Total pipeline']:.2f}s")

    return timings


if __name__ == "__main__":
    spark = create_spark_session()
    try:
        process_data(spark)
    finally:
        spark.stop()