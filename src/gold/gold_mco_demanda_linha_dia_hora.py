import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    to_timestamp,
    concat_ws,
    hour,
    sum as fsum,
    when,
    lit
)

SILVER_PATH = "data/silver/mco/2024"
GOLD_PATH = "data/gold/mco_demanda_linha_dia_hora"
DELTA_JAR = "io.delta:delta-spark_2.12:3.2.0"


def build_spark():
    """Create and configure Spark session with Delta Lake support."""
    return (
        SparkSession.builder
        .appName("gold-mco-demanda")
        .config("spark.driver.memory", "4g")
        .config("spark.jars.packages", DELTA_JAR)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )


def reset(path: str):
    """Remove directory if it exists."""
    if os.path.exists(path):
        shutil.rmtree(path)


def main():
    """Main ETL function to transform silver to gold layer."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Read silver data
    df = spark.read.format("delta").load(SILVER_PATH)

    # Create date column from "viagem" timestamp
    df = df.withColumn("dia", to_date(col("viagem")))

    # Create timestamp from departure (date + saida HH:mm)
    saida_ts = to_timestamp(
        concat_ws(" ", col("dia").cast("string"), col("saida")),
        "yyyy-MM-dd HH:mm"
    )

    # Extract hour from departure timestamp
    df = df.withColumn(
        "hora_saida",
        when(saida_ts.isNotNull(), hour(saida_ts)).otherwise(lit(None))
    )

    # Aggregate passenger demand by day, hour and bus line
    df_gold = (
        df
        .groupBy("dia", "hora_saida", "linha")
        .agg(fsum(col("total_usuarios")).alias("total_usuarios"))
    )

    # Write gold data
    reset(GOLD_PATH)
    df_gold.write.mode("overwrite").format("delta").save(GOLD_PATH)

    print("Gold MCO demanda por linha/dia/hora concluída.")
    spark.stop()


if __name__ == "__main__":
    main()