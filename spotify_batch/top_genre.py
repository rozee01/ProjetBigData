from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, row_number
from pyspark.sql.window import Window
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from mongo_cnx import save_to_mongo

def process_spotify_data(input_path):
    spark = SparkSession.builder \
        .appName("SpotifyTopGenreAllTime") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    # Ensure streams and genre columns are valid
    df_cleaned = df \
        .filter(col("streams").isNotNull()) \
        .filter(col("country").isNotNull()) \
        .filter(col("artist_genre").isNotNull()) \
        .filter(col("streams").rlike(r"^\d+(\.\d+)?$")) \
        .filter(~col("country").rlike(r"^\d+(\.\d+)?$")) \
        .withColumn("streams", col("streams").cast("double").cast("long")) \
        .withColumn("country", col("country").cast("string")) \
        .withColumn("artist_genre", col("artist_genre").cast("string")).withColumnRenamed("artist_genre", "genre")
    # Group and aggregate
    grouped_df = df_cleaned \
        .groupBy("country", "genre") \
        .agg(_sum("streams").alias("total_streams"))

    # Rank within each country
    window = Window.partitionBy("country").orderBy(col("total_streams").desc())

    ranked_df = grouped_df \
        .withColumn("rank", row_number().over(window)) \
        .filter(col("rank") == 1) \
        .drop("rank")

    # Collect results
    result = ranked_df.toPandas().to_dict("records")
    spark.stop()
    return result



if __name__ == "__main__":
    input_path = "spotify_cleaned.csv"
    results = process_spotify_data(input_path)
    print(results[:5])  # Print first 5 results for verification
    save_to_mongo(results,'top_genre_all_time')