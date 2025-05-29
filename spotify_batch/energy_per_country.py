from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from mongo_cnx import save_to_mongo

def process_avg_audio_features(input_path):
    spark = SparkSession.builder \
        .appName("AverageDanceabilityEnergyPerCountry") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    # Filter only rows where danceability and energy are valid numbers
    df_clean = df \
        .filter(col("country").isNotNull()) \
        .filter(col("danceability").isNotNull()) \
        .filter(col("energy").isNotNull()) \
        .filter(col("danceability").rlike(r"^\d+(\.\d+)?$")) \
        .filter(col("energy").rlike(r"^\d+(\.\d+)?$")) \
        .withColumn("danceability", col("danceability").cast("double")) \
        .withColumn("energy", col("energy").cast("double"))

    avg_df = df_clean.groupBy("country").agg(
        avg("danceability").alias("avg_danceability"),
        avg("energy").alias("avg_energy")
    )

    result = avg_df.toPandas().to_dict("records")
    spark.stop()
    return result

if __name__ == "__main__":
    input_path = "spotify_cleaned.csv"
    result_df = process_avg_audio_features(input_path)
    save_to_mongo(result_df,"energy_per_country")
