from pyspark.sql import SparkSession
import folium
from folium.plugins import HeatMap

DELTA_JAR = "io.delta:delta-spark_2.12:3.2.0"
HEATMAP_PATH = "data/gold/onibus_heatmap"


def main() -> None:
    """Create a heatmap visualization from bus position data."""
    # Initialize Spark session with Delta Lake support
    spark = (
        SparkSession.builder
        .appName("visualiza-heatmap")
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

    try:
        # Read heatmap data from Delta table
        df = spark.read.format("delta").load(HEATMAP_PATH)

        # Optional: filter for a specific hour
        # df = df.filter("hora = '2020-07-29 14:00:00'")

        # Convert to pandas for visualization
        pdf = df.select("grid_lat", "grid_lon", "pontos").toPandas()

        # Create map centered on Belo Horizonte
        mapa = folium.Map(location=[-19.92, -43.94], zoom_start=12)

        # Prepare data for heatmap
        heat_data = [
            [row["grid_lat"], row["grid_lon"], row["pontos"]]
            for _, row in pdf.iterrows()
        ]

        # Add heatmap layer
        HeatMap(heat_data, radius=12).add_to(mapa)

        # Save map to HTML file
        mapa.save("heatmap_onibus.html")
        print("Heatmap salvo como heatmap_onibus.html")

    finally:
        # Ensure Spark session is stopped even if an error occurs
        spark.stop()


if __name__ == "__main__":
    main()