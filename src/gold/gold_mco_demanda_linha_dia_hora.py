import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, concat_ws, hour, sum as fsum, when, lit
)

SILVER_PATH = "data/silver/mco/2024"
GOLD_PATH = "data/gold/mco_demanda_linha_dia_hora"
DELTA_JAR = "io.delta:delta-spark_2.12:3.2.0"


def build_spark():
    return (
        SparkSession.builder
        .appName("gold-mco-demanda")
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

    # dia baseado em "viagem" (timestamp)
    df = df.withColumn("dia", to_date(col("viagem")))

    # tenta criar um timestamp da saída (dia + saida HH:mm)
    saida_ts = to_timestamp(concat_ws(" ", col("dia").cast("string"), col("saida")), "yyyy-MM-dd HH:mm")

    df = df.withColumn(
        "hora_saida",
        when(saida_ts.isNotNull(), hour(saida_ts)).otherwise(lit(None))
    )

    df_gold = (
        df
        .groupBy("dia", "hora_saida", "linha")
        .agg(fsum(col("total_usuarios")).alias("total_usuarios"))
    )

    reset(GOLD_PATH)
    df_gold.write.mode("overwrite").format("delta").save(GOLD_PATH)

    print("Gold MCO demanda por linha/dia/hora concluída.")
    spark.stop()


if __name__ == "__main__":
    main()
