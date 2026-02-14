import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, countDistinct

SILVER_PATH = "data/silver/tempo_real_onibus"
GOLD_PATH = "data/gold/onibus_ativos_por_linha_dia"
DELTA_JAR = "io.delta:delta-spark_2.12:3.2.0"


def build_spark():
    return (
        SparkSession.builder
        .appName("gold-onibus-ativos")
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

    df_gold = (
        df
        .withColumn("dia", to_date(col("timestamp")))
        .groupBy("dia", "linha")
        .agg(countDistinct("veiculo").alias("veiculos_ativos"))
    )

    reset(GOLD_PATH)
    df_gold.write.mode("overwrite").format("delta").save(GOLD_PATH)

    print("Gold ônibus ativos por linha/dia concluída.")
    spark.stop()


if __name__ == "__main__":
    main()
