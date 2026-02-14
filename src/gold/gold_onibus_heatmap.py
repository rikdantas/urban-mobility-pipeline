import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_trunc, floor, count, countDistinct, round as fround
)

SILVER_PATH = "data/silver/tempo_real_onibus"
GOLD_PATH = "data/gold/onibus_heatmap"
DELTA_JAR = "io.delta:delta-spark_2.12:3.2.0"

GRID_SIZE = 0.01  # ~1.1km


def build_spark():
    return (
        SparkSession.builder
        .appName("gold-onibus-heatmap")
        .config("spark.driver.memory", "4g")
        .config("spark.jars.packages", DELTA_JAR)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def reset(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(SILVER_PATH)

    df_grid = (
        df
        .withColumn("hora", date_trunc("hour", col("timestamp")))
        .withColumn("grid_lat", fround(floor(col("lat") / GRID_SIZE) * GRID_SIZE, 4))
        .withColumn("grid_lon", fround(floor(col("lon") / GRID_SIZE) * GRID_SIZE, 4))
        .groupBy("hora", "grid_lat", "grid_lon")
        .agg(
            count("*").alias("pontos"),
            countDistinct("veiculo").alias("veiculos_distintos"),
        )
    )

    reset(GOLD_PATH)
    df_grid.write.mode("overwrite").format("delta").save(GOLD_PATH)

    print("Gold ônibus heatmap concluída.")
    spark.stop()


if __name__ == "__main__":
    main()
