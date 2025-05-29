import pandas as pd
from kafka import KafkaProducer
import json
import time
import os

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Your Kafka brokers
KAFKA_TOPIC_NAME = 'nowplaying_tweets'      # Kafka topic to send messages to
SIMULATION_DELAY_SECONDS = 1                # Delay between sending messages (seconds)

# Determine the absolute path to the CSV file
# Assumes the script is in ProjetBigData/now-playing-streamer/simulator/tweet_simulator.py
# And the data CSV is in ProjetBigData/data/context_content_features.csv
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..')) # Moves up two levels to ProjetBigData
    CSV_FILE_PATH = os.path.join(PROJECT_ROOT, 'data', 'context_content_features.csv')
except NameError:
    # __file__ is not defined if running in some interactive environments
    # Fallback or prompt user if necessary, for now, use a relative path that might work if run from project root
    CSV_FILE_PATH = os.path.join('data', 'context_content_features.csv')
    print("Warning: Could not determine script directory. Assuming CSV is in 'data/context_content_features.csv' relative to execution path.")


def create_kafka_producer():
    """Creates and returns a Kafka producer."""
    print(f"Attempting to connect to Kafka brokers at {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5, # Retry sending messages on failure
            request_timeout_ms=30000 # Wait up to 30s for acks
        )
        print("Kafka Producer connected successfully.")
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        print("Please ensure Kafka is running and accessible.")
        return None

def simulate_tweet_stream(producer, csv_path):
    """
    Reads data from the CSV and simulates a tweet stream by sending messages to Kafka.
    """
    if not producer:
        print("Kafka producer not available. Exiting simulation.")
        return

    print(f"Attempting to load CSV file from: {os.path.abspath(csv_path)}")
    try:
        df = pd.read_csv(csv_path, on_bad_lines='skip', low_memory=False)
        print(f"Successfully loaded {len(df)} records from {csv_path}")
    except FileNotFoundError:
        print(f"Error: CSV file not found at {os.path.abspath(csv_path)}.")
        print("Please ensure you have downloaded 'context_content_features.csv' from Kaggle")
        print("and placed it in the 'data' directory within your project root (ProjetBigData/data/).")
        return
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    print(f"Starting tweet simulation to Kafka topic: {KAFKA_TOPIC_NAME}...")
    for index, row in df.iterrows():
        # Construct the message from CSV row
        # Based on Kaggle dataset description for context_content_features.csv
        message = {
            "event_id": row.get('id'),
            "user_id": row.get('user_id'),
            "track_id": row.get('track_id'),
            "artist_id": row.get('artist_id'),
            "created_at": row.get('created_at'),
            "tweet_text_entities": row.get('entities'), # Using 'entities' as a proxy for tweet text/content
            "tweet_language": row.get('tweet_language'),
            "user_language": row.get('user_lang'),
            "time_zone": row.get('time_zone'),
            # GeoJSON fields
            "coordinates_geojson": row.get('coordinates'),
            "place_geojson": row.get('place'),
            "geo_geojson": row.get('geo'),
            # Music content features
            "music_instrumentalness": row.get('instrumentalness'),
            "music_liveness": row.get('liveness'),
            "music_speechiness": row.get('speechiness'),
            "music_danceability": row.get('danceability'),
            "music_valence": row.get('valence'),
            "music_loudness": row.get('loudness'),
            "music_tempo": row.get('tempo'),
            "music_acousticness": row.get('acousticness'),
            "music_energy": row.get('energy'),
            "music_mode": row.get('mode'),
            "music_key": row.get('key')
        }
        
        # Remove keys with None or NaN values to keep JSON clean
        message_cleaned = {k: v for k, v in message.items() if pd.notna(v)}

        try:
            future = producer.send(KAFKA_TOPIC_NAME, value=message_cleaned)
            # Optional: Wait for send confirmation (can slow down simulation)
            # record_metadata = future.get(timeout=10)
            # print(f"Sent message: event_id {message_cleaned.get('event_id')} to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            if index % 100 == 0: # Print progress every 100 messages
                 print(f"Sent message {index+1}/{len(df)}: event_id {message_cleaned.get('event_id')}")

        except Exception as e:
            print(f"Error sending message (event_id: {message_cleaned.get('event_id')}) to Kafka: {e}")
            # Decide if to break or continue
            # break 
            
        time.sleep(SIMULATION_DELAY_SECONDS)

    try:
        producer.flush(timeout=60) # Wait up to 60s for all messages to be sent
        print("All messages flushed to Kafka.")
    except Exception as e:
        print(f"Error flushing messages to Kafka: {e}")
        
    print("Finished simulating all tweets.")
    producer.close()
    print("Kafka producer closed.")

if __name__ == "__main__":
    print("Tweet Simulator Script Starting...")
    
    kafka_producer = create_kafka_producer()
    
    if kafka_producer:
        simulate_tweet_stream(kafka_producer, CSV_FILE_PATH)
