import os
import json
import requests
from pyspark.sql import SparkSession

BRONZE_PATH = "data/bronze"

TEMPO_REAL_API = "https://dados.pbh.gov.br/api/3/action/datastore_search?resource_id=88101fac-7200-4476-8c4f-09e1663c435e"

MCO_APIS = {
    "01": "https://dados.pbh.gov.br/api/3/action/datastore_search?resource_id=4b427f5b-f3f2-4a72-bb2f-f7b779e36894",
    "02": "https://dados.pbh.gov.br/api/3/action/datastore_search?resource_id=90d84484-02dc-4cc7-b278-3437b7afe69f",
}

MCO_CSVS = {
    "03": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/resource/e331e109-1e4f-40e2-83d5-5b45e5b87cfd/download/mco-03-2024.csv",
    "04_05_06": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/resource/2e72b12b-ece2-49d1-bd4a-d0e07b7fec9f/download/mco04-06-2024-.csv",
    "07": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/resource/85085eff-db10-4f24-8553-80306c842276/download/mco-07-2024.csv",
    "08": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/resource/9d5950ab-4ef4-4e77-bea8-20f4eb2f54f7/download/mco-08-2024.csv",
    "09": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/resource/74f557a3-2f26-4edc-89f4-b39b69d9bf98/download/mco-09-2024.csv",
    "10": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/resource/b7f9cf4d-f85b-4ee0-b558-b20173783ac9/download/mco-10-2024.csv",
    "11": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/resource/0528e305-7df9-490b-987d-9649ef486052/download/mco-11-2024.csv",
    "12": "https://ckan.pbh.gov.br/dataset/7ae4d4b4-6b52-4042-b021-0935a1db3814/resource/212a6f6b-4716-455c-bf11-f4330e0bfbd6/download/mco-12-2024.csv",
}

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def fetch_api(url):
    headers = {
        "User-Agent": "urban-mobility-pipeline/1.0"
    }

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    data = r.json()

    if not data.get("success"):
        raise RuntimeError("API CKAN retornou success=false")

    return data["result"]["records"]


def download_csv(url, path):
    headers = {
        "User-Agent": "urban-mobility-pipeline/1.0"
    }

    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def main():
    spark = (
    SparkSession.builder
    .appName("urban-pipeline")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
    )

    # ================= TEMPO REAL =================
    print("Ingestão tempo real ônibus")

    records = fetch_api(TEMPO_REAL_API)
    df = spark.createDataFrame(records)

    out = f"{BRONZE_PATH}/tempo_real_onibus"
    ensure_dir(out)
    df.write.mode("overwrite").parquet(out)

    # ================= MCO API (01/02) =================
    for mes, url in MCO_APIS.items():
        print(f"MCO {mes} via API")

        records = fetch_api(url)
        df = spark.createDataFrame(records)

        out = f"{BRONZE_PATH}/mco/2024/mco_{mes}"
        ensure_dir(out)
        df.write.mode("overwrite").parquet(out)

    # ================= MCO CSV =================
    tmp = "/tmp/mco"
    ensure_dir(tmp)

    for mes, url in MCO_CSVS.items():
        print(f"MCO {mes} via CSV")

        local = f"{tmp}/mco_{mes}.csv"
        download_csv(url, local)

        df = spark.read.option("header", True).csv(local)

        out = f"{BRONZE_PATH}/mco/2024/mco_{mes}"
        ensure_dir(out)
        df.write.mode("overwrite").parquet(out)

    print("Bronze concluída.")

if __name__ == "__main__":
    main()
