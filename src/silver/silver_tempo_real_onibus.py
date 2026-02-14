import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, concat_ws, to_timestamp, when, lit

BRONZE_PATH = "data/bronze/tempo_real_onibus"
SILVER_PATH = "data/silver/tempo_real_onibus"
DELTA_JAR = "io.delta:delta-spark_2.12:3.2.0"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver-tempo-real-onibus")
        .config("spark.driver.memory", "4g")
        .config("spark.jars.packages", DELTA_JAR)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def reset_output_dir(path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(BRONZE_PATH)

    if len(df.columns) < 3:
        raise RuntimeError(f"Bronze inesperado: esperava >=3 colunas, veio {len(df.columns)}")

    c1, c2, c3 = df.columns[:3]

    p1 = split(col(c1), ";")
    p2 = split(col(c2), ";")
    p3 = split(col(c3), ";")

    # Coordenadas (reconstrução correta)
    lat_int = p1.getItem(2)   # ex: -19
    lat_dec = p2.getItem(0)   # ex: 950163
    lon_int = p2.getItem(1)   # ex: -43
    lon_dec = p3.getItem(0)   # ex: 971882

    # Campos (mapeados a partir do layout real observado)
    veiculo = p3.getItem(1)               # ex: 30193
    velocidade_raw = p3.getItem(3).cast("int")  # ex: 636 (interpretação: 63.6 km/h)

    # Normalização de velocidade:
    # - Se estiver 0..150, usa direto
    # - Senão tenta /10 e valida 0..150
    velocidade_kmh = (
        when((velocidade_raw >= 0) & (velocidade_raw <= 150), velocidade_raw.cast("double"))
        .when(((velocidade_raw / lit(10.0)) >= 0) & ((velocidade_raw / lit(10.0)) <= 150),
              (velocidade_raw / lit(10.0)))
        .otherwise(lit(None))
    )

    df_silver = (
        df
        .withColumn("linha", p1.getItem(0))
        .withColumn("timestamp", to_timestamp(p1.getItem(1), "yyyyMMddHHmmss"))
        .withColumn("lat", concat_ws(".", lat_int, lat_dec).cast("double"))
        .withColumn("lon", concat_ws(".", lon_int, lon_dec).cast("double"))
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

    # Evita erro de merge de schema durante desenvolvimento local
    reset_output_dir(SILVER_PATH)

    df_silver.write.format("delta").mode("overwrite").save(SILVER_PATH)

    print("Silver tempo real ônibus concluída com sucesso.")
    spark.stop()


if __name__ == "__main__":
    main()
