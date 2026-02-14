import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as fsum

SILVER_PATH = "data/silver/mco/2024"
GOLD_PATH = "data/gold/mco_top_linhas"
DELTA_JAR = "io.delta:delta-spark_2.12:3.2.0"


def build_spark():
    """Create and configure Spark session with Delta Lake support."""
    return (
        SparkSession.builder
        .appName("gold-mco-top-linhas")
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
    """Main ETL function to aggregate top bus lines by passenger demand."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Read silver data
    df = spark.read.format("delta").load(SILVER_PATH)

    # Aggregate total passengers by bus line and sort descending
    df_gold = (
        df.groupBy("linha")
        .agg(fsum(col("total_usuarios")).alias("total_usuarios"))
        .orderBy(col("total_usuarios").desc())
    )

    # Write gold data
    reset(GOLD_PATH)
    df_gold.write.mode("overwrite").format("delta").save(GOLD_PATH)

    print("Gold MCO top linhas concluída.")
    spark.stop()


if __name__ == "__main__":
    main()