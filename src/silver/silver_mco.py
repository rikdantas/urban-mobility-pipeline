import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import IntegerType

BRONZE_BASE = "data/bronze/mco/2024"
SILVER_PATH = "data/silver/mco/2024"
DELTA_JAR = "io.delta:delta-spark_2.12:3.2.0"


def build_spark() -> SparkSession:
    """Create and configure Spark session with Delta Lake support."""
    return (
        SparkSession.builder
        .appName("silver-mco")
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


def list_mco_partitions(base_dir: str) -> list[str]:
    """List all mco_* subdirectories in the base directory."""
    if not os.path.exists(base_dir):
        raise FileNotFoundError(f"Diretório não encontrado: {base_dir}")

    paths = []
    for name in sorted(os.listdir(base_dir)):
        full_path = os.path.join(base_dir, name)
        if os.path.isdir(full_path) and name.lower().startswith("mco_"):
            paths.append(full_path)

    if not paths:
        raise RuntimeError(f"Nenhuma subpasta mco_* encontrada em: {base_dir}")

    return paths


def main() -> None:
    """Main ETL function to transform bronze MCO data to silver layer."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Read all monthly partitions
    mco_paths = list_mco_partitions(BRONZE_BASE)
    df = spark.read.parquet(*mco_paths)

    # =========================================================
    # Remove columns with invalid names for Delta (ex.: "", " ")
    # =========================================================
    invalid_cols = [col_name for col_name in df.columns if col_name.strip() == ""]
    if invalid_cols:
        df = df.drop(*invalid_cols)

    # Standardize column names (remove leading spaces)
    rename_map = {
        " VIAGEM": "viagem",
        " LINHA": "linha",
        " SUBLINHA": "sublinha",
        " PC": "pc",
        " CONCESSIONARIA": "concessionaria",
        " SAIDA": "saida",
        " VEICULO": "veiculo",
        " CHEGADA": "chegada",
        " CATRACA SAIDA": "catraca_saida",
        " CATRACA CHEGADA": "catraca_chegada",
        " OCORRENCIA": "ocorrencia",
        " JUSTIFICATIVA": "justificativa",
        " TIPO DIA": "tipo_dia",
        " EXTENSAO": "extensao",
        " FALHA MECANICA": "falha_mecanica",
        " EVENTO INSEGURO": "evento_inseguro",
        " INDICADOR FECHAMENTO": "indicador_fechamento",
        " DATA FECHAMENTO": "data_fechamento",
        " TOTAL USUARIOS": "total_usuarios",
        " EMPRESA OPERADORA": "empresa_operadora",
    }

    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    # Cast numeric columns to integer type
    integer_columns = [
        "catraca_saida",
        "catraca_chegada",
        "total_usuarios",
        "extensao",
        "tipo_dia",
        "pc",
        "empresa_operadora",
        "concessionaria",
        "sublinha",
        "veiculo",
    ]
    for column in integer_columns:
        if column in df.columns:
            df = df.withColumn(column, col(column).cast(IntegerType()))

    # Parse timestamp columns
    if "viagem" in df.columns:
        df = df.withColumn(
            "viagem",
            to_timestamp(col("viagem"), "dd/MM/yyyy")
        )

    if "data_fechamento" in df.columns:
        df = df.withColumn(
            "data_fechamento",
            to_timestamp(col("data_fechamento"), "dd/MM/yyyy HH:mm")
        )

    # Write silver data in Delta format
    reset_output_dir(SILVER_PATH)
    df.write.mode("overwrite").format("delta").save(SILVER_PATH)

    print("Silver MCO concluída com sucesso.")
    spark.stop()


if __name__ == "__main__":
    main()