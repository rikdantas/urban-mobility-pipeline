import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, concat_ws, to_timestamp, when, lit

BRONZE_PATH = "data/bronze/tempo_real_onibus"
SILVER_PATH = "data/silver/tempo_real_onibus"
DELTA_JAR = "io.delta:delta-spark_2.12:3.2.0"


def build_spark() -> SparkSession:
    """Create and configure Spark session with Delta Lake support."""
    return (
        SparkSession.builder
        .appName("silver-tempo-real-onibus")
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


def reset_output_dir(path: str) -> None:
    """Remove directory if it exists."""
    if os.path.exists(path):
        shutil.rmtree(path)


def main() -> None:
    """Main ETL function to transform bronze real-time bus data to silver layer."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Read bronze data
    df = spark.read.parquet(BRONZE_PATH)

    # Validate bronze structure
    if len(df.columns) < 3:
        raise RuntimeError(
            f"Bronze inesperado: esperava >=3 colunas, veio {len(df.columns)}"
        )

    # Extract first three columns for parsing
    col1, col2, col3 = df.columns[:3]

    # Split semicolon-separated values
    split_col1 = split(col(col1), ";")
    split_col2 = split(col(col2), ";")
    split_col3 = split(col(col3), ";")

    # Extract coordinate components
    lat_int = split_col1.getItem(2)   # ex: -19
    lat_dec = split_col2.getItem(0)   # ex: 950163
    lon_int = split_col2.getItem(1)   # ex: -43
    lon_dec = split_col3.getItem(0)   # ex: 971882

    # Extract vehicle and speed fields
    veiculo = split_col3.getItem(1)               # ex: 30193
    velocidade_raw = split_col3.getItem(3).cast("int")  # ex: 636 (interpretation: 63.6 km/h)

    # Normalize speed values:
    # - If between 0-150, use as is
    # - Otherwise try dividing by 10 and validate 0-150 range
    velocidade_kmh = (
        when(
            (velocidade_raw >= 0) & (velocidade_raw <= 150),
            velocidade_raw.cast("double")
        )
        .when(
            ((velocidade_raw / lit(10.0)) >= 0) &
            ((velocidade_raw / lit(10.0)) <= 150),
            (velocidade_raw / lit(10.0))
        )
        .otherwise(lit(None))
    )

    # Transform to silver schema
    df_silver = (
        df
        .withColumn("linha", split_col1.getItem(0))
        .withColumn(
            "timestamp",
            to_timestamp(split_col1.getItem(1), "yyyyMMddHHmmss")
        )
        .withColumn(
            "lat",
            concat_ws(".", lat_int, lat_dec).cast("double")
        )
        .withColumn(
            "lon",
            concat_ws(".", lon_int, lon_dec).cast("double")
        )
        .withColumn("veiculo", veiculo)
        .withColumn("velocidade_kmh", velocidade_kmh)
        .select(
            "linha",
            "timestamp",
            "lat",
            "lon",
            "veiculo",
            "velocidade_kmh",
        )
    )

    # Clear output directory before writing
    reset_output_dir(SILVER_PATH)

    # Write silver data in Delta format
    df_silver.write.format("delta").mode("overwrite").save(SILVER_PATH)

    print("Silver tempo real ônibus concluída com sucesso.")
    spark.stop()


if __name__ == "__main__":
    main()