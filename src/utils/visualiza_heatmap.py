from pyspark.sql import SparkSession
import folium
from folium.plugins import HeatMap

DELTA_JAR = "io.delta:delta-spark_2.12:3.2.0"
HEATMAP_PATH = "data/gold/onibus_heatmap"

spark = (
    SparkSession.builder
    .appName("visualiza-heatmap")
    .config("spark.jars.packages", DELTA_JAR)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

df = spark.read.format("delta").load(HEATMAP_PATH)

# Opcional: filtrar uma hora específica
# df = df.filter("hora = '2020-07-29 14:00:00'")

pdf = df.select("grid_lat", "grid_lon", "pontos").toPandas()

spark.stop()

# Criar mapa centrado em BH
mapa = folium.Map(location=[-19.92, -43.94], zoom_start=12)

heat_data = [
    [row["grid_lat"], row["grid_lon"], row["pontos"]]
    for _, row in pdf.iterrows()
]

HeatMap(heat_data, radius=12).add_to(mapa)

mapa.save("heatmap_onibus.html")

print("Heatmap salvo como heatmap_onibus.html")
