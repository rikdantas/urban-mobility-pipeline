import os
import requests
from pyspark.sql import SparkSession

BRONZE_PATH = "data/bronze"

# Tempo real ônibus (API CKAN)
TEMPO_REAL_API = (
    "https://dados.pbh.gov.br/api/3/action/datastore_search"
    "?resource_id=88101fac-7200-4476-8c4f-09e1663c435e"
)

# MCO 2024 (todos CSV)
MCO_CSVS = {
    "01": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/"
          "resource/429dcd1d-80b8-4be3-a729-397029b75192/download/mco-01-2023.csv",
    "02": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/"
          "resource/90d84484-02dc-4cc7-b278-3437b7afe69f/download/mco-02-2024.csv",
    "03": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/"
          "resource/e331e109-1e4f-40e2-83d5-5b45e5b87cfd/download/mco-03-2024.csv",
    "04_05_06": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/"
                "resource/2e72b12b-ece2-49d1-bd4a-d0e07b7fec9f/download/mco04-06-2024-.csv",
    "07": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/"
          "resource/85085eff-db10-4f24-8553-80306c842276/download/mco-07-2024.csv",
    "08": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/"
          "resource/9d5950ab-4ef4-4e77-bea8-20f4eb2f54f7/download/mco-08-2024.csv",
    "09": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/"
          "resource/74f557a3-2f26-4edc-89f4-b39b69d9bf98/download/mco-09-2024.csv",
    "10": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/"
          "resource/b7f9cf4d-f85b-4ee0-b558-b20173783ac9/download/mco-10-2024.csv",
    "11": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/"
          "resource/0528e305-7df9-490b-987d-9649ef486052/download/mco-11-2024.csv",
    "12": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/"
          "resource/212a6f6b-4716-455c-bf11-f4330e0bfbd6/download/mco-12-2024.csv",
}

HEADERS = {"User-Agent": "urban-mobility-pipeline/1.0"}


def ensure_dir(path):
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)


def fetch_api(url):
    """Fetch data from CKAN API and return records."""
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    data = response.json()

    if not data.get("success"):
        raise RuntimeError("CKAN retornou success=false")

    return data["result"]["records"]


def download_csv(url, path):
    """Download CSV file from URL to local path."""
    with requests.get(url, headers=HEADERS, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(path, "wb") as file:
            for chunk in response.iter_content(8192):
                file.write(chunk)


def main():
    """Main ETL function to ingest data into bronze layer."""
    spark = (
        SparkSession.builder
        .appName("urban-bronze")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    # ================= TEMPO REAL ÔNIBUS =================
    print("Ingestão tempo real ônibus")

    records = fetch_api(TEMPO_REAL_API)
    df_onibus = spark.createDataFrame(records)

    out_onibus = f"{BRONZE_PATH}/tempo_real_onibus"
    ensure_dir(out_onibus)

    df_onibus.write.mode("overwrite").parquet(out_onibus)

    # ================= MCO CSV =================
    tmp_dir = "/tmp/mco"
    ensure_dir(tmp_dir)

    for mes, url in MCO_CSVS.items():
        print(f"MCO {mes}")

        local_file = f"{tmp_dir}/mco_{mes}.csv"
        download_csv(url, local_file)

        df = (
            spark.read
            .option("header", True)
            .option("delimiter", ";")  # <- ESSENCIAL
            .csv(local_file)
        )

        out_dir = f"{BRONZE_PATH}/mco/2024/mco_{mes}"
        ensure_dir(out_dir)

        df.write.mode("overwrite").parquet(out_dir)

    spark.stop()
    print("Bronze concluída.")


if __name__ == "__main__":
    main()