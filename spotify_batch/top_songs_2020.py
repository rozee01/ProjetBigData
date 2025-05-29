from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, row_number
from pyspark.sql.window import Window
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from mongo_cnx import save_to_mongo

def process_spotify_data(input_path):
    spark = SparkSession.builder \
        .appName("SpotifyTopSong2020") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    df_2020 = df \
        .filter(col("release_date").startswith("2020")) \
        .filter(col("streams").isNotNull()) \
        .withColumn("streams", col("streams").cast("double").cast("long"))
    
    grouped_df = df_2020 \
        .groupBy("country", "track_name") \
        .agg(_sum("streams").alias("total_streams"))

    window = Window.partitionBy("country").orderBy(col("total_streams").desc())

    ranked_df = grouped_df \
        .withColumn("rank", row_number().over(window)) \
        .filter(col("rank") == 1) \
        .drop("rank")

    # Collect to driver to insert into MongoDB
    result = ranked_df.toPandas().to_dict("records")
    spark.stop()
    return result


if __name__ == "__main__":

    input_path = "spotify_cleaned.csv"  
    results = process_spotify_data(input_path)
    save_to_mongo(results,"top_song_2020")
