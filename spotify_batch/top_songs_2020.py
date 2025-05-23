from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, row_number
from pyspark.sql.window import Window
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os
load_dotenv()  # Load variables from .env

username = os.getenv("MONGO_USERNAME")
password = os.getenv("MONGO_PASSWORD")
def connect_to_mongo():
    connection_string = f"mongodb+srv://{username}:{password}@projetbigdata.bg6dron.mongodb.net/?retryWrites=true&w=majority&appName=ProjetBigData"
    return MongoClient(connection_string, server_api=ServerApi("1"))

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

def save_to_mongo(records):
    client = connect_to_mongo()
    db = client.ProjetBigDataDB # You can choose any DB name
    collection = db["top_song_2020"]
    collection.drop()  # optional: clear previous results
    collection.insert_many(records)
    print("✅ Data saved to MongoDB.")

if __name__ == "__main__":

    input_path = "spotify_cleaned.csv"  
    results = process_spotify_data(input_path)
    save_to_mongo(results)
