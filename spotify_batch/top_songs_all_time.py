from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, row_number
from pyspark.sql.window import Window
from mongo_cnx import save_to_mongo

def process_spotify_data(input_path):
    spark = SparkSession.builder \
        .appName("SpotifyTopSongAllTime") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    # df.printSchema()
    # df.show(5)
    # Ensure streams column is valid and convert to long
    df_cleaned = df \
        .filter(col("streams").isNotNull()) \
        .filter(col("country").isNotNull()) \
        .filter(col("track_name").isNotNull()) \
        .filter(col("uri").isNotNull()) \
        .filter(col("streams").rlike(r"^\d+(\.\d+)?$")) \
        .filter(~col("country").rlike(r"^\d+(\.\d+)?$")) \
        .withColumn("streams", col("streams").cast("double").cast("long")) \
        .withColumn("country", col("country").cast("string")) \
        .withColumn("track_name", col("track_name").cast("string")) \
        .withColumn("uri", col("uri").cast("string"))

    # Group and aggregate
    grouped_df = df_cleaned \
        .groupBy("country", "track_name","uri") \
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
    #print(results[:5])  
    save_to_mongo(results,"top_songs_all_time")
