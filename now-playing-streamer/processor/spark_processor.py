import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, MapType
from pymongo import MongoClient
from dotenv import load_dotenv

# --- Configuration ---
# KAFKA_BOOTSTRAP_SERVERS is now set via spark-submit --conf
KAFKA_TOPIC_NAME = 'nowplaying_tweets'
MONGO_COLLECTION_NAME = 'processed_tweets'

# Load environment variables from .env file
# Assumes .env file is in the project root (ProjetBigData)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..')) # Moves up two levels to ProjetBigData
DOTENV_PATH = os.path.join(PROJECT_ROOT, '.env')
load_dotenv(dotenv_path=DOTENV_PATH)

MONGO_URI = os.getenv('MONGO_URI')

if not MONGO_URI:
    print("Error: MONGO_URI not found in .env file.")
    print(f"Please ensure .env file exists at {DOTENV_PATH} and contains MONGO_URI.")
    exit(1)

# Define the schema for the incoming Kafka messages (must match producer)
# Based on tweet_simulator.py message structure
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("artist_id", StringType(), True),
    StructField("created_at", StringType(), True), # Assuming it's a string, can be parsed to TimestampType later
    StructField("tweet_text_entities", StringType(), True), # This was 'entities' in the CSV
    StructField("tweet_language", StringType(), True),
    StructField("user_language", StringType(), True),
    StructField("time_zone", StringType(), True),
    StructField("coordinates_geojson", StringType(), True), # GeoJSON often stored as string or MapType
    StructField("place_geojson", StringType(), True),
    StructField("geo_geojson", StringType(), True),
    StructField("music_instrumentalness", DoubleType(), True),
    StructField("music_liveness", DoubleType(), True),
    StructField("music_speechiness", DoubleType(), True),
    StructField("music_danceability", DoubleType(), True),
    StructField("music_valence", DoubleType(), True),
    StructField("music_loudness", DoubleType(), True),
    StructField("music_tempo", DoubleType(), True),
    StructField("music_acousticness", DoubleType(), True),
    StructField("music_energy", DoubleType(), True),
    StructField("music_mode", StringType(), True), # Mode can be 0 or 1, or Major/Minor
    StructField("music_key", StringType(), True) # Key can be integer or string like 'C#'
])

def process_batch(df, epoch_id):
    """Processes a micro-batch: enriches data and writes to MongoDB."""
    if df.rdd.isEmpty(): # More reliable check for empty DataFrame in streaming
        print(f"Epoch {epoch_id}: No data in this batch.")
        return

    print(f"Epoch {epoch_id}: Processing {df.count()} records...")

    # --- Placeholder for Enrichment Logic ---
    # Example: Add a processing timestamp
    df_enriched = df.withColumn("processing_timestamp_utc", current_timestamp())

    # Convert DataFrame to list of dictionaries for MongoDB insertion
    # Using toJSON to handle complex types like GeoJSON strings correctly if they are valid JSON
    # Note: If geojson fields are not actual JSON strings, this might need adjustment
    # For direct dict conversion: records_to_insert = [row.asDict(recursive=True) for row in df_enriched.collect()]
    
    # Collect data to driver. Be mindful of large datasets.
    collected_rows = df_enriched.collect()
    if not collected_rows:
        print(f"Epoch {epoch_id}: No records to insert after processing and collection.")
        return

    records_to_insert = [row.asDict(recursive=True) for row in collected_rows]

    try:
        client = MongoClient(MONGO_URI)
        db_name_parts = MONGO_URI.split('/')
        db_name = 'big_data_project' # Default DB name
        if len(db_name_parts) > 3:
            db_name_candidate = db_name_parts[-1].split('?')[0]
            if db_name_candidate: # Ensure it's not empty if URI ends with /
                db_name = db_name_candidate
        
        db = client[db_name]
        collection = db[MONGO_COLLECTION_NAME]
        
        result = collection.insert_many(records_to_insert)
        print(f"Epoch {epoch_id}: Successfully inserted {len(result.inserted_ids)} records into MongoDB collection '{MONGO_COLLECTION_NAME}' in database '{db_name}'.")
        client.close()
    except Exception as e:
        print(f"Epoch {epoch_id}: Error writing to MongoDB: {e}")
        # Potentially add more robust error handling, e.g., retries or dead-letter queue

def main():
    print("Starting Spark Streaming processor...")

    # MONGO_URI check can happen early
    if not MONGO_URI:
        print("CRITICAL: MONGO_URI not found. Please set it in your .env file.")
        print(f"Attempted to load .env from: {DOTENV_PATH}")
        return

    # Initialize SparkSession
    spark = SparkSession.builder.appName("RealtimeTweetProcessor") \
        .config("spark.mongodb.output.uri", MONGO_URI) \
        .getOrCreate()

    # Now that 'spark' is initialized, we can get its configuration
    kafka_brokers_from_conf = spark.conf.get("spark.kafka.bootstrap.servers", "N/A")
    print(f"Reading from Kafka topic: {KAFKA_TOPIC_NAME} at {kafka_brokers_from_conf}")
    print(f"Writing to MongoDB collection: {MONGO_COLLECTION_NAME}")
    print("Ensure Kafka and MongoDB are running and accessible.")
    print(f"Ensure .env file is correctly set up with MONGO_URI at {DOTENV_PATH}")
    print("Spark Kafka and MongoDB connector JARs will be downloaded if not present (requires internet).")
    print("To run this, use: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 spark_processor.py")
    print("Replace Spark/connector versions with those compatible with your Spark installation.")
    print("-----------------------------------------------------------------------")

    spark = (
        SparkSession.builder.appName("NowPlayingTweetProcessor")
        # Packages are now typically added during spark-submit, but can be here too.
        # .config("spark.jars.packages", 
        #         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," + 
        #         "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") 
        .config("spark.sql.shuffle.partitions", "4") # Adjust based on your cluster/local setup
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN") # Reduce verbosity

    # Read from Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("kafka.bootstrap.servers", "kafka:29092") # Explicitly set for the Docker environment
        .option("startingOffsets", "latest")  # Process new messages. Use "earliest" to process all existing.
        .option("failOnDataLoss", "false") # Continue if some Kafka offsets are lost (e.g. topic recreated)
        .load()
    )

    # Parse the value column (which is JSON) into structured data
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Process each micro-batch using the defined function
    query = (
        parsed_df.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .trigger(processingTime='30 seconds')  # Process data every 30 seconds
        .start()
    )

    print("Streaming query started. Waiting for termination (Ctrl+C to stop)...")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Streaming query interrupted by user. Stopping...")
    finally:
        print("Stopping Spark session...")
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    main()
