from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, row_number
from pyspark.sql.window import Window
from mongo_cnx import save_to_mongo

def process_spotify_data(input_path):
    spark = SparkSession.builder \
        .appName("SpotifyTopArtistPerGenre") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    # Ensure streams, genre, artist, and artist_id columns are valid
    df_cleaned = df \
        .filter(col("streams").isNotNull()) \
        .filter(col("artist_genre").isNotNull()) \
        .filter(col("artist_individual").isNotNull()) \
        .filter(col("artist_id").isNotNull()) \
        .filter(col("streams").rlike(r"^\d+(\.\d+)?$")) \
        .withColumn("streams", col("streams").cast("double").cast("long")) \
        .withColumn("genre", col("artist_genre").cast("string")) \
        .withColumn("artist", col("artist_individual").cast("string")) \
        .withColumn("artist_id", col("artist_id").cast("string")).filter(col("artist_id").startswith("spotify:artist"))

    # Group and aggregate
    grouped_df = df_cleaned \
        .groupBy("genre", "artist", "artist_id") \
        .agg(_sum("streams").alias("total_streams"))


    # Rank within each genre
    window = Window.partitionBy("genre").orderBy(col("total_streams").desc())

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
    save_to_mongo(results)