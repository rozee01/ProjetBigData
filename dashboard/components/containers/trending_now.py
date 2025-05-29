import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os
import time
import random
from kafka import KafkaConsumer
import json
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
from mongo_cnx import connect_to_mongo

def render_trending_now():
    # Connect to MongoDB
    db = connect_to_mongo()
    collection = db["daily_charts"]

    # Load data
    df = pd.DataFrame(list(collection.find({}, {"_id": 0})))
    st.subheader("Trending Songs of today: ")
    st.dataframe(df.sort_values("Rank"))

    # Load track names from dataset.csv
    dataset_df = pd.read_csv("data/dataset.csv")
    all_track_names = dataset_df["track_name"].dropna().tolist()

    # Kafka consumer setup
    consumer = KafkaConsumer(
        'result',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    st.subheader("#NowPlaying (Live Kafka Stream)")

    # Store received tracks
    tracks = []

    # Placeholders for live updates
    live_text = st.empty()
    live_table = st.empty()

    try:
        for message in consumer:
            track_data = message.value  # Expected: {"track_name": "Song Name"}
            track = track_data.get("track_id", "Unknown Track")
            random_name = random.choice(all_track_names)

            # Append to list
            tracks.append({"track ID": track, "track name":random_name,"timestamp": time.strftime("%H:%M:%S")})

            # Show latest track
            live_text.markdown(f"**🎶 Now Playing: {track}**")

            # Update DataFrame
            df_tracks = pd.DataFrame(tracks)
            live_table.dataframe(df_tracks)

            time.sleep(2)  # control update rate

    except Exception as e:
        st.error(f"Error reading from Kafka: {e}")


    
